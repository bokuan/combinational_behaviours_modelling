import sqlalchemy
from sqlalchemy.engine import Engine
from getpass import getpass
import datetime
import pandas as pd
from typing import Union
import os

def write_df_to_db(df: pd.DataFrame, table_name: str, schema: str = 'da_dev',
                   history: bool = False, append: bool = True, write_date: bool = True) -> None:
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
    history : bool , optional
        Boolean that tells the function whether or not to create a _history
        table / append to the _history table. True = create/append History
        defaults to False
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

    write_engine = _create_write_user()

    date = datetime.datetime.now().strftime('%Y/%m/%d_%H:%M:%S')
    if write_date:
        df['write_to_db_date'] = [date] * len(df)

    if append:
        replace_or_append = 'append'
    else:
        replace_or_append = 'replace'

    df.to_sql(table_name, write_engine, if_exists=replace_or_append,
              schema=schema, index=False)

    if history:
        table_hist = table_name + '_history'
        df.to_sql(table_hist, write_engine, if_exists='append',
                  schema=schema, index=False)
    write_engine.dispose()


def read_db_to_df(table_name: str, schema: str = 'adas_dm') -> pd.DataFrame:
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

    read_engine = _create_read_user()
    conn = read_engine.connect()
    metadata_obj = sqlalchemy.MetaData()
    
    # engine level:
    
    # create tables
    metadata_obj.create_all(read_engine)
    
    # reflect all tables
    metadata_obj.reflect(read_engine)
    table = sqlalchemy.Table(table_name, metadata_obj, autoload_with=read_engine, schema=schema)

    query = sqlalchemy.select(table)
    df = conn.execute(query)
    df = pd.DataFrame(df)
    df.columns = table.columns.keys()

    conn.close()
    read_engine.dispose()

    return df


def select_rows_or_check_existence(table_name: str, schema: str,
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

    values_exist = []
    select_df = pd.DataFrame()
    read_engine = _create_read_user()
    conn = read_engine.connect()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata, autoload=True,
                             autoload_with=read_engine, schema=schema)

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
                table.columns[identifying_columns] == identifying_values[0])
            temp_df = conn.execute(select_query)
            temp_df = pd.DataFrame(temp_df)

        # checking aginst multiple columns is a little trickier
        else:
            select_query_expression = 'sqlalchemy.select([table])'
            for i, _ in enumerate(identifying_columns):
                select_query_expression += \
                    '.where(table.columns[identifying_columns[{}]]\
                         == identifying_values[{}])'.format(i, i)
            temp_df = eval('conn.execute('+select_query_expression+')')
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
                temp_df = conn.execute(select_query)
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
                temp_df = eval('conn.execute('+select_query_expression+')')
                temp_df = pd.DataFrame(temp_df)

                if temp_df.shape[0] > 0:
                    values_exist.append(True)
                else:
                    values_exist.append(False)
            select_df = pd.concat([select_df, temp_df])

    conn.close()
    read_engine.dispose()

    # returning either the df or existence truth values depending on input
    if select:
        select_df.columns = table.columns.keys()
        return select_df
    else:
        return values_exist


def replace_rows(table_name: str, schema: str,
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

    write_engine = _create_write_user()
    conn = write_engine.connect()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata, autoload=True,
                             autoload_with=write_engine, schema=schema)

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
            conn.execute(del_query)

        # otherwise we have a more complicate query
        else:
            del_query_expression = 'sqlalchemy.delete(table)'
            for i, _ in enumerate(identifying_removal_columns):
                del_query_expression += '.where(table.c[identifying_removal_columns[{}]]\
                     == identifying_removal_values[{}])'.format(i, i)
            eval('conn.execute('+del_query_expression+')')

        # add a datetime row if we're 1 row short of the table length
        if len(new_rows) == len(table.columns.keys()) - 1:
            new_rows.append(datetime.datetime.now().strftime(
                '%Y/%m/%d_%H:%M:%S'))

        # insert our new rows in
        ins_query = table.insert().values(new_rows)
        conn.execute(ins_query)
        conn.close()
        write_engine.dispose()

    # If there is more than one set of conditions to remove rows based on
    elif type(identifying_removal_values) == pd.DataFrame or entry_zero_type == list:

        for i, _ in enumerate(identifying_removal_values):
            if type(identifying_removal_columns) == str:
                del_query = sqlalchemy.delete(table).where(
                    table.c[identifying_removal_columns]
                    == identifying_removal_values[0])
                conn.execute(del_query)

            else:
                del_query_expression = 'sqlalchemy.delete(table)'
                for j in range(len(identifying_removal_columns)):
                    del_query_expression += '.where(table.c[identifying_removal_columns[{}]]\
                    == identifying_removal_values[{}][{}])'.format(j, i, j)
                eval('conn.execute('+del_query_expression+')')

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
        conn.execute(ins_query)
        conn.close()
        write_engine.dispose()


def replace_values_on_key(table_name: str, schema: str,
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

    write_engine = _create_write_user()
    conn = write_engine.connect()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata, autoload=True,
                             autoload_with=write_engine, schema=schema)

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
            conn.execute(update_query)

        # checking against multiple columns
        else:
            update_query_expression = 'sqlalchemy.update(table)'
            for i, _ in enumerate(identifying_columns):
                update_query_expression += '.where(table.c[identifying_columns[{}]] == identifying_vals[{}])'.format(
                    i, i)
            update_query_expression += '.values('+str(update_dict)+')'
            eval('conn.execute('+update_query_expression+')')

    # if we have many sets of identifying values to look at
    elif type(identifying_vals) == pd.DataFrame or entry_zero_type == list:

        # loop through the sets of identifying values
        for i, _ in enumerate(identifying_vals):

            # one column to check against case
            if type(identifying_columns) == str:
                update_query = sqlalchemy.update(table).where(
                    table.c[identifying_columns]
                    == identifying_vals[0]).values(update_dict)
                conn.execute(update_query)

            # multiple columns to check values agaisnt
            else:
                update_query_expression = 'sqlalchemy.update(table)'
                for j in range(len(identifying_columns)):
                    update_query_expression += '.where(table.c[identifying_columns[{}]] == identifying_vals[{}][{}])'.format(
                        j, i, j)
                update_query_expression += '.values('+str(update_dict)+')'
                eval('conn.execute('+update_query_expression+')')

    conn.close()
    write_engine.dispose()


def _create_read_user():
    read_engine = _create_engine_adas()
    return read_engine


def _create_write_user():
    write_engine = _create_engine_adas()
    return write_engine


def _create_engine_adas() -> Engine:
    """_summary_

    Parameters
    ----------
    access : str, optional
        Logs in as read or write user, by default 'read'
    user : str, optional
        Overrides access parameter if supplied, by default ''
    password : str, optional
        the password for corresponding user, by default ''
    server : str, optional
        internal for usage on dsl, external otherwise by default ''

    Returns
    -------
    Engine
       The sqlalchemy engine we use to connect to the database

    Raises
    ------
    KeyError
        Wrong access type specified
    """

    engine = __create_engine()
    return engine


def __create_engine() -> Engine:

    server = "dsl-functions.ess.volvo.net"
    database = "gtt_vasp_got"

    max_retries = 3
    retries = 0

    # Get current User ID
    username = os.environ["USER"]

    while retries < max_retries:
        password = getpass("Please enter your password: ")
        try:
            # Connect to your postgres DB
            conn_str = ('postgresql://'
                        + username + ':'
                        + password + '@'
                        + server + '/'
                        + database)
        
            engine = sqlalchemy.create_engine(
                conn_str, paramstyle="format")
            print("Connection successful")
            return engine
        except Exception as e:
            print(f"An error occurred: {e}")

            retries += 1
            if retries < max_retries:
                print("Try again.")
            else:
                print("Exiting.")
                return None
