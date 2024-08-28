import sqlalchemy
import sys
import datetime
import pytz
import pandas as pd
import getpass
import json
import os

from sqlalchemy.engine import Engine, URL
from typing import Union, List


class InvalidConfig(Exception):
    def __init__(self, message):
        print(message)


class NoConfig(Exception):
    def __init__(self, message):
        print(message)


class DBConnector():

    def __init__(self, db_config: dict = {}, db_config_path: str = ''):
        self._vcn = getpass.getuser()
        self._max_retries = 3

        self._engine = None
        self.engine_status = 'not connected'
        self.engine_connected = 'not connected'

        self.__init_db_config_from_arg(db_config)
        self._db_config_path = db_config_path

        self.__get_db_config()
        self.__create_engine()

    def __init_db_config_from_arg(self, db_config):
        if not db_config:
            self._db_config = {'server': 'dsl-functions.ess.volvo.net',
                               'db': 'gtt_vasp_got'}
        else:
            self._db_config = db_config

    def __get_db_config(self):
        if self._db_config_path:
            self.__check_config_path()
        else:
            self.__validate_config()

    def __validate_config(self):

        keys = self._db_config.keys()

        if (('db' not in keys) or ('server' not in keys)):
            raise InvalidConfig(
                'db and server are required fields in your config')

        if not (('user' in keys) and ('pass' in keys)):
            print('A user and password were not both provided; using your vcn to login, you will be prompted for a password')
            self._db_config['user'] = self._vcn
        self._db_config['user'] = self._db_config['user'].lower()

    def __check_config_path(self):
        try:
            with open(self._db_config_path) as f:
                self._db_config = json.load(f)
                self.__validate_config()

        except FileNotFoundError:
            raise NoConfig(
                'Did not find the config path, and no config was provided; exiting')

    def __create_engine(self) -> Engine:
        if "AIRFLOW_CTX_DAG_ID" in os.environ:
            self.__adjust_config_for_airflow()
        self.__create_config_engine()

    def __adjust_config_for_airflow(self):
        from airflow.hooks.base_hook import BaseHook

        connection = BaseHook.get_connection("CONN_SCHEDULING_SERVICE_ACCOUNT")
        self._db_config['user'] = connection.login.lower()
        self._db_config['pass'] = connection.password

    def __create_config_engine(self):
        self.__get_user_pass()
        url = URL.create("postgresql",
                         username=self._db_config['user'],
                         password=self._db_config['pass'],
                         host=self._db_config['server'],
                         database=self._db_config['db'])
        self.__clear_user_pass()
        self._engine = sqlalchemy.create_engine(url)
        self._engine_status = 'connected'

    def __get_user_pass(self):
        if 'pass' not in self._db_config.keys() and self._db_config['user'] == self._vcn:
            self._db_config['pass'] = getpass.getpass()

    def __clear_user_pass(self):
        if self._db_config['user'] == self._vcn:
            del self._db_config['pass']

    def _create_connection(self):
        retries = 0
        while retries < self._max_retries:

            try:
                self._conn = self._engine.connect()
                self.engine_connected = 'connected'
                break

            except Exception as e:
                print(f"An error occured: {e}")
                print(f"Retry number: {retries+1}")
                self.__create_config_engine()
                retries += 1

    def close(self):
        self._close_connection()
        self._dispose_of_engine()

    def _close_connection(self):
        self._conn.close()
        self.engine_connected = 'not connected'

    def _dispose_of_engine(self):
        self._engine.dispose()
        self._engine_status = 'not connected'


class DBWrapper(DBConnector):

    def __init__(self, db_config: dict = {}, db_config_path: str = ''):
        super().__init__(db_config=db_config, db_config_path=db_config_path)
        self._create_connection()

    def _reconnect_if_conn_closed(self):
        if (self._conn.closed) and (self.engine_connected == 'connected'):
            self._conn = self._engine.connect()

    def write_df_to_db(self, df: pd.DataFrame, table_name: str, schema: str = 'da_dev',
                       append: bool = True, write_date: bool = True) -> None:
        """
        Takes the dataframe and writes it to the database
        It will be written under your desired name. The table will by
        default append, if you want to replace set append=False. This
        is so you don't accidentally overwrite your table

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe that you want to send to the database
            Note that there are weird exceptions with variable types that you can
            put into the database, you'll probably get errors occaisonally
        table_name : str
            The name of the table you want to save your data to.
            The df will also be appended to a table with _history
            after your desired table name.
        schema : str
            The name of the schema that the table will go to
            defaults to 'adas_dm'
        append : bool , optional
            appends on the table_name table rather than replacing if True
            History table functionality is unaffected
            defaults to False
        write_date : bool, optional
            Dictates whether or not a write_to_db_date column is added
            main use case is when we want to add a column to a table so
            we load in the table, add column, and overwrite, we want to keep
            the same write_to_db_date as before rather than overwriting with
            the current date.
            defaults to True
        Returns
        -------
            Returns None, the point is to write to the database
        """

        self._reconnect_if_conn_closed()

        date = datetime.datetime.now().strftime('%Y/%m/%d_%H:%M:%S')
        if write_date:
            df.loc[:, 'write_to_db_date'] = date

        if append:
            replace_or_append = 'append'
        else:
            replace_or_append = 'replace'

        df.to_sql(table_name, self._engine, if_exists=replace_or_append,
                  schema=schema, index=False)

        self._log_action('write_df_to_db', ['table_name: ' + table_name,
                                            'schema: ' + schema,
                                            'append: ' + str(append),
                                            'write_date: ' + str(write_date),
                                            ]
                         )

    def read_db_to_df(self, table_name: str, schema: str = 'adas_dm') -> pd.DataFrame:
        """reads a table from the database and outputs a dataframe

        Parameters
        ----------
        table_name : str
            the name of the table you want to read
        schema : str, optional
            the schema that the table resides in
            by default 'adas_dm'

        Returns
        -------
        pd.DataFrame
            The Dataframe version of the table that we read (columns included)
        """

        self._reconnect_if_conn_closed()

        metadata_obj = sqlalchemy.MetaData()

        # engine level:

        # create tables
        metadata_obj.create_all(self._engine)

        # reflect all tables
        metadata_obj.reflect(self._engine)
        table = sqlalchemy.Table(table_name, metadata_obj,
                                 autoload_with=self._engine, schema=schema)

        query = sqlalchemy.select(table)
        df = self._conn.execute(query)
        df = pd.DataFrame(df)
        df.columns = table.columns.keys()

        return df

    def select_rows_or_check_existence(self, table_name: str, schema: str,
                                       identifying_values:
                                       Union[str, int, list, pd.DataFrame],
                                       identifying_columns:
                                       Union[str, list],
                                       select:
                                       bool = True) ->\
            Union[list, pd.DataFrame]:
        """
        Checks if the values you want exist in the specified columns
        in the table that you want to look in. Or returns the dataframe
        of all rows given your identifiers

        Parameters
        ----------
        table_name : str
            name of the table you want to check or select from
        schema : str
            schema the table is located in
        identifying_values : Union[list, pd.DataFrame]
            the values in a list or a list of lists or a dataframe of values you
            want to check existence of or grab the rows of within the table
        identifying_columns : Union[str, list]
            the names of the columns that the identifying values should be checked
            against
            Note that this cannot be a list of lists or dataframe, e.g. for
            each separate row it is not designed to check for different
            identifying columns. i.e. the identifying_values will all be
            checked for existence using the same set of identifying
            columns (str or a list of column names)
        select : bool
            Determines whether or not the output of the function is the dataframe
            of rows corresponding to your identifiers, or just a list of bools
            signifying whether they exist in the first place.

        Returns
        -------
        Union[list, pd.DataFrame]
            Booleans of whether the value sets exist in a single row for the set
            of columns you specified. Or if select=True it returns the dataframe
            containing all rows matching your criteria
        """

        self._reconnect_if_conn_closed()

        values_exist = []
        select_df = pd.DataFrame()
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, metadata, autoload=True,
                                 autoload_with=self._engine, schema=schema)

        # Check if we have a list of lists or dataframe
        try:
            entry_zero_type = type(identifying_values[0])
        except:
            entry_zero_type = []

        # Turning identifying values into a list for compatibility
        if type(identifying_values) != list and type(identifying_values) != pd.DataFrame:
            identifying_values = [identifying_values]

        # Checking if we have a list of values, but not a list of lists or dataframe
        if type(identifying_values) == list and entry_zero_type != list:

            # Simple query if we are only dealing with one column to check against
            if type(identifying_columns) == str:
                select_query = sqlalchemy.select([table]).where(
                    getattr(table.columns, identifying_columns) == identifying_values[0])
                temp_df = self._conn.execute(select_query)
                temp_df = pd.DataFrame(temp_df)

            # checking aginst multiple columns is a little trickier
            else:
                select_query_expression = 'sqlalchemy.select([table])'
                for i, _ in enumerate(identifying_columns):
                    select_query_expression += \
                        '.where(table.columns[identifying_columns[{}]]\
                            == identifying_values[{}])'.format(i, i)
                temp_df = eval(
                    'self._conn.execute('+select_query_expression+')')
                temp_df = pd.DataFrame(temp_df)

            if temp_df.shape[0] > 0:
                values_exist.append(True)
            else:
                values_exist.append(False)

            select_df = pd.concat([select_df, temp_df])

        # If we have a data frame or a list of lists (e.g. checking for multiple conditions)
        elif type(identifying_values) == pd.DataFrame or entry_zero_type == list:

            # Need to unpack to list for database
            try:
                identifying_values = identifying_values.values.tolist()
            except:
                pass

            # Check for each set of identifying values whether they exist
            for i, _ in enumerate(identifying_values):
                if type(identifying_columns) == str:
                    select_query = sqlalchemy.select([table]).where(
                        table.c[identifying_columns] == identifying_values[i][0])
                    temp_df = self._conn.execute(select_query)
                    temp_df = pd.DataFrame(temp_df)

                    if temp_df.shape[0] > 0:
                        values_exist.append(True)
                    else:
                        values_exist.append(False)
                else:
                    select_query_expression = 'sqlalchemy.select([table])'
                    for j in range(len(identifying_columns)):
                        select_query_expression += '.where(table.c[identifying_columns[{}]]\
                            == identifying_values[{}][{}])'.format(j, i, j)
                    temp_df = eval(
                        'self._conn.execute('+select_query_expression+')')
                    temp_df = pd.DataFrame(temp_df)

                    if temp_df.shape[0] > 0:
                        values_exist.append(True)
                    else:
                        values_exist.append(False)
                select_df = pd.concat([select_df, temp_df])

        # returning either the df or existence truth values depending on input
        if select:
            select_df.columns = table.columns.keys()
            return select_df
        else:
            return values_exist

    def replace_rows(self, table_name: str, schema: str,
                     new_rows: Union[list, pd.DataFrame],
                     identifying_removal_values: Union[int, list, pd.DataFrame],
                     identifying_removal_columns: Union[str, list]) -> None:
        """
        Replaces rows in a dataframe based on any number of identifying columns
        identifying columns refer to datatable entries which must share those
        values with the replacing rows respective index for said column.
        example: 'test' is the 3rd column of your table, let's say it is your
        identifying_column then only values where the 'test' column values match
        the 3rd entry of your replacement row will be deleted and your replacement
        row will be added to the table.

        **P.S. If there is no match your row will still be added to the table
        **P.P.S This script looks at each row you are "replacing" individually,
                if you pass in more than one row it is the same as passing in
                all of those rows separately.

        Parameters
        ----------
        table_name : str
            name of the table you are replacing rows in
        schema : str
            the schema said table is located in
        new_rows : Union[list, pd.DataFrame]
            your date that you want to add in, replacing other rows
            given your identifying_columns conditions
        identifying_columns : Union[str, list]
            The set of columns whose matching values for a given row
            dictate which rows from the table will be deleted and replaced
            by your desired rows.

        Returns
        -------
        list
        a list of bools telling you whether or not your values exist
        in the table
        """

        self._reconnect_if_conn_closed()

        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, metadata, autoload=True,
                                 autoload_with=self._engine, schema=schema)

        # if its not a list make it a list for compatibility
        if type(identifying_removal_values) != list and type(identifying_removal_values) != pd.DataFrame:
            identifying_removal_values = [identifying_removal_values]
        elif type(identifying_removal_values) == pd.DataFrame:
            identifying_removal_values = identifying_removal_values.tolist()

        # Checking whether we have list of lists or dataframe
        try:
            entry_zero_type = type(identifying_removal_values[0])
        except:
            entry_zero_type = []

        # If we only have one row that we are removing based on
        if type(identifying_removal_values) == list and entry_zero_type != list:

            # query for a single column to identify against
            if type(identifying_removal_columns) == str:
                del_query = sqlalchemy.delete(table).where(
                    table.c[identifying_removal_columns]
                    == identifying_removal_values[0])
                self._conn.execute(del_query)

            # otherwise we have a more complicate query
            else:
                del_query_expression = 'sqlalchemy.delete(table)'
                for i, _ in enumerate(identifying_removal_columns):
                    del_query_expression += '.where(table.c[identifying_removal_columns[{}]]\
                        == identifying_removal_values[{}])'.format(i, i)
                eval('self._conn.execute('+del_query_expression+')')

            # add a datetime row if we're 1 row short of the table length
            if len(new_rows) == len(table.columns.keys()) - 1:
                new_rows.append(datetime.datetime.now().strftime(
                    '%Y/%m/%d_%H:%M:%S'))

            # insert our new rows in
            ins_query = table.insert().values(new_rows)
            self._conn.execute(ins_query)

        # If there is more than one set of conditions to remove rows based on
        elif type(identifying_removal_values) == pd.DataFrame or entry_zero_type == list:

            for i, _ in enumerate(identifying_removal_values):
                if type(identifying_removal_columns) == str:
                    del_query = sqlalchemy.delete(table).where(
                        table.c[identifying_removal_columns]
                        == identifying_removal_values[0])
                    self._conn.execute(del_query)

                else:
                    del_query_expression = 'sqlalchemy.delete(table)'
                    for j in range(len(identifying_removal_columns)):
                        del_query_expression += '.where(table.c[identifying_removal_columns[{}]]\
                        == identifying_removal_values[{}][{}])'.format(j, i, j)
                    eval('self._conn.execute('+del_query_expression+')')

            # adding the write_df_to_db_date column
            if type(new_rows) == pd.DataFrame:
                new_rows['write_df_to_db_date'] = datetime.datetime.now().strftime(
                    '%Y/%m/%d_%H:%M:%S')
                new_rows = new_rows.values.tolist()
            else:
                try:
                    write_date = datetime.datetime.now(
                    ).strftime('%Y/%m/%d_%H:%M:%S')
                    temp = []
                    for i, _ in enumerate(new_rows):
                        cur_list = new_rows[i]
                        cur_list.append(write_date)
                        temp.append(cur_list)
                    new_rows = temp
                except:
                    pass

            ins_query = table.insert().values(new_rows)
            self._conn.execute(ins_query)

            self._log_action('replace_rows', ['table_name: ' + table_name,
                                              'schema: ' + schema,
                                              'new_rows: ' + str(new_rows),
                                              'identifying_removal_columns: ' +
                                              str(identifying_removal_columns),
                                              'identifying_removal_values: ' +
                                              str(identifying_removal_values)
                                              ]
                             )

    def replace_values_on_key(self, table_name: str, schema: str,
                              update_dict: dict,
                              identifying_vals: Union[int, str, list],
                              identifying_columns: Union[str, list]):
        """
        Updates all values in a set of given columns according to your dictionary
        e.g. {'column1': value1, 'column2': value2 ...} , all values in column 1
        will be replaced with value1, and column2 will be replaced with value2
        based on your identifying criteria. Meaning only the rows whose values
        match the values from your identifying_vals and identifying_columns
        will be updated according to your dictionary.

        Parameters
        ----------
        table_name : str
            name of the table you want to update
        schema : str
            location of the table in the database
        update_dict : dict
            dictionary containing the column names following the values you want
            to have those columns updated by
        identifying_vals : Union[int, str, list]
            the values that will determine which rows are suitable to update
        identifying_columns : Union[str, list]
            the columns that we will parse for the identifying values you
            have passed in
        """

        self._reconnect_if_conn_closed()

        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, metadata, autoload=True,
                                 autoload_with=self._engine, schema=schema)

        # if its not a list make it a list for compatibility
        if type(identifying_vals) != list and type(identifying_vals) != pd.DataFrame:
            identifying_vals = [identifying_vals]
        elif type(identifying_vals) == pd.DataFrame:
            identifying_vals = identifying_vals.tolist()

        # checking if list of lists
        try:
            entry_zero_type = type(identifying_vals[0])
        except:
            entry_zero_type = []

        # if we have a list of id vals, and its not a list of lists
        if type(identifying_vals) == list and entry_zero_type != list:

            # if we only check against one column it's easy
            if type(identifying_columns) == str:
                update_query = sqlalchemy.update(table).where(
                    table.c[identifying_columns]
                    == identifying_vals[0]).values(update_dict)
                self._conn.execute(update_query)

            # checking against multiple columns
            else:
                update_query_expression = 'sqlalchemy.update(table)'
                for i, _ in enumerate(identifying_columns):
                    update_query_expression += '.where(table.c[identifying_columns[{}]] == identifying_vals[{}])'.format(
                        i, i)
                update_query_expression += '.values('+str(update_dict)+')'
                eval('self._conn.execute('+update_query_expression+')')

        # if we have many sets of identifying values to look at
        elif type(identifying_vals) == pd.DataFrame or entry_zero_type == list:

            # loop through the sets of identifying values
            for i, _ in enumerate(identifying_vals):

                # one column to check against case
                if type(identifying_columns) == str:
                    update_query = sqlalchemy.update(table).where(
                        table.c[identifying_columns]
                        == identifying_vals[0]).values(update_dict)
                    self._conn.execute(update_query)

                # multiple columns to check values agaisnt
                else:
                    update_query_expression = 'sqlalchemy.update(table)'
                    for j in range(len(identifying_columns)):
                        update_query_expression += '.where(table.c[identifying_columns[{}]] == identifying_vals[{}][{}])'.format(
                            j, i, j)
                    update_query_expression += '.values('+str(update_dict)+')'
                    eval('self._conn.execute('+update_query_expression+')')

        self._log_action('replace_values_on_key', ['table_name: ' + table_name,
                                                   'schema: ' + schema,
                                                   'update_dict: ' +
                                                   str(update_dict),
                                                   'identifying_columns: ' +
                                                   str(identifying_columns),
                                                   'identifying_values: ' +
                                                   str(identifying_vals)
                                                   ]
                         )

    def _log_action(self, func_called: str, passed_args: List[str]):
        if not ("AIRFLOW_CTX_DAG_ID" in os.environ):
            got_tz = pytz.timezone("Europe/Stockholm")
            today = datetime.datetime.utcnow().astimezone(got_tz)

            current_date = today.strftime("%Y-%m-%d")
            current_time = today.strftime("%Y-%m-%d:%H-%M-%S")

            splitter = '\n' + '-'*50 + '\n'
            lines_to_log = [current_time, self._vcn,
                            func_called] + passed_args + [splitter]

            # Append to the days log file
            with open('/mnt/vasp-got-da/scripts/team_data_analytics/logs/db_logs/'+current_date+'.log', 'a') as f:
                f.writelines(line + '\n' for line in lines_to_log)
