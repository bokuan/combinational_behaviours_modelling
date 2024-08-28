import pandas as pd
import glob
import os

from src.db import db_helper_dsl3 as dbh


def append_metadata_from_jpt3_sheet(table_name:str='metadata_test_track', schema:str='da_regressions',
                                    sheet_path:str='/mnt/va_vasp1.1/2. Joint Periodic Testing/JPT3/jpt3_metadata.xlsx'):
    """
    Temporary function for appending JPT3 metadata from excel sheet to the testtrack metadata table.

    Ugly fix in this to first load the current database, and appending this sheet to it. Done to avoid issues with
    new columns that did not exist in the original schema. Long term solution should 
    1. Not have an excel sheet with annotations
    2. Have same columns available from any metadata parser
    3. Never overwrite (just add non-duplicated rows) to the table

    Parameters
    ----------
    table_name : str, optional
        Table name to write to, by default 'metadata_test_track'
    schema : str, optional
        Schema name to write to, by default 'da_regressions'
    sheet_path : str, optional
        Path to annotation sheet, by default '/mnt/va_vasp1.1/2. Joint Periodic Testing/JPT3/jpt3_metadata.xlsx'
    """
    jpt3_metadata_path = sheet_path
    df_jpt3 = pd.read_excel(jpt3_metadata_path)

    # clean up format
    df_jpt3 = df_jpt3.drop(columns=['Column1'])

    # Combine existing database with the new data
    df_old = dbh.read_db_to_df(table_name=table_name, schema=schema)
    df_combined = pd.concat([df_old, df_jpt3])

    # Write the metadata from sheet to the table
    dbh.write_df_to_db(df_combined, table_name=table_name,
                       schema=schema, append=False, write_date=False)


def write_metadata_from_jpt4(file_path:str, table_name='metadata_test_track', schema='da_regressions', append:bool=False):
    """
    Save parsed and organized metadata from JPT4 data annotation files, located in the given path.

    Parameters
    ----------
    file_path : str
        Path to the files from JPT4.
    table_name : str, optional
        Table name to write to, by default 'metadata_test_track'
    schema : str, optional
        Schema name to write to, by default 'da_regressions'
    append : bool, optional
        Append to database or not?, by default False
    """

    # parse the annotation files, currently only for MOIS
    metadata_df = parse_annotations_jpt4(file_path)

    # clean up the metadata and add important things such as dbc-channel mapping, and so on
    metadata_df = metadata_df.drop_duplicates(subset='recording_id')
    metadata_df = metadata_df[['recording_id', 'annotation_filename', 'test_function', 'test_activity',
                            'scenario', 'side', 'target_type', 'target_speed', 'target_distance', 'pass/fail',
                            'start_recording', 'stop_recording',  'full_annotation']]

    metadata_df['vehicle_id'] = metadata_df.recording_id.str.split('_').str[0]
    metadata_df['test_date'] = metadata_df.recording_id.str.split('_').str[1]
    metadata_df['test_date'] = pd.to_datetime(metadata_df['test_date'])

    dbc_mapping_jpt4 = ('CAN1 : Backbone1J1939-T2_1.28.0, CAN2 : Backbone2-T2_1.28.0,' +
                        ' CAN3 : VicinityNet1-T2_1.27.0, CAN4 : VicinityNet4-T2_1.28.0,' +
                        ' CAN5 : VicinityNet2-T2_1.28.0, CAN6 (python 5) : AB_Volvo_w2218.dbc')
    metadata_df['channel_dbc_mapping'] = dbc_mapping_jpt4

    # TODO Could add more things here, for example the blf file name corresponding to it, other things from metadata etc. Include things from manual annotations?

    dbh.write_df_to_db(metadata_df, table_name=table_name,
                    schema=schema, append=append, write_date=False)


def parse_annotations_jpt4(folder_path:str) -> pd.DataFrame:
    """
    Parser for annotations from JPT4 - currently only adapted for MOIS logs.

    This will parse each annotation file in the folder that is given, line by line, and return as a dataframe.
    The parser saves each type of information as a specified metadata field. However, this logic needs some work and relys on how 
    well the annotations are made.
    Recommendation: Only send in MOIS path right now, to be efficient. All others will give nonsense output.

    Parameters
    ----------
    folder_path : str
        Path to folder with data (structured according to AVL logger).

    Returns
    -------
    pd.DataFrame
        Dataframe with metadata from the testcases.
    """

    files = glob.glob(os.path.join(folder_path, '**/*.blf'), recursive=True)
    annot_files = glob.glob(os.path.join(
        folder_path, '**/*.txt'), recursive=True)

    metadatas = []
    for file in annot_files:
        metadata = {}
        with open(file) as f:
            metadata['annotation_filename'] = file
            lines = f.readlines()

            metadata['test_function'] = lines[0].strip()
            metadata['test_activity'] = 'JPT4'  # ugly fix again

            # find start and stoptime of recording:
            for l in lines:
                _ = _check_line(l, metadata, metadata['test_function'])
            # print(metadata)
            metadatas.append(metadata)

    metadata_df = pd.DataFrame(metadatas)
    metadata_df['recording_id'] = metadata_df['annotation_filename'].str.rsplit(
        '/').str[-3] + '_00'

    # TODO add in the corresponding blf filename? it should be easy with the glob mentioned above.

    return metadata_df


def _check_line(line: str, metadata: dict, function: str):
    """
    Parser for the annotation text files from test vehicles.

    Function takes in a metadata dictionary and a new line in the annoation file, and updates the 
    information accordingly.
    Very limited usecase - currently only MOIS, and is likely to not work if annotation method changes

    Parameters
    ----------
    line : str
        New line to parse from text file.
    metadata : dict
        Dictionary with the current metadata.
    function : str
        String with the EUF, which will decide how the information is stored.

    Returns
    -------
    dict
        Dictionary updated with the parsed metadata
    """

    # print(line)
    if 'recording has started' in line.lower():
        metadata['start_recording'] = line.rsplit(':', 1)[0].strip()
    if 'recording has stopped' in line.lower():
        metadata['stop_recording'] = line.rsplit(':', 1)[0].strip()
    if 'ok' in line.lower() or 'nok' in line.lower():
        if 'pass/fail' in metadata.keys():
            metadata['pass/fail'] = metadata['pass/fail'] + \
                ', ' + line.rsplit(':', 1)[1].strip()
        else:
            metadata['pass/fail'] = line.rsplit(':', 1)[1].strip()

    if 'full_annotation' in metadata.keys():
        metadata['full_annotation'] = metadata['full_annotation'] + ' ' + line
    else:
        metadata['full_annotation'] = line

    if function == 'MOIS':
        if 'bike' in line.lower() or 'pedestrian' in line.lower() or ('ped' in line.lower() and not 'stopped' in line.lower()):
            metadata['target_type'] = 'bike'
        if 'pedestrian' in line.lower() or ('ped' in line.lower() and not 'stopped' in line.lower()):
            metadata['target_type'] = 'pedestrian'
        if 'target' in line.lower() and 'right' in line.lower():
            metadata['scenario'] = 'crossing'
            metadata['side'] = 'right'
        elif 'right' in line.lower():
            metadata['side'] = 'right'

        if 'target' in line.lower() and 'left' in line.lower():
            metadata['scenario'] = 'crossing'
            metadata['side'] = 'left'

        elif 'left' in line.lower():
            metadata['side'] = 'left'
            # will miss some longitudinal cases with this statement, but at least not destroy the crossing ones

        if 'center' in line.lower():
            metadata['scenario'] = 'longitudinal'
            metadata['side'] = 'center'
            metadata['target_type'] = 'bike'

        if 'follow bike' in line.lower():
            metadata['scenario'] = 'longitudinal'
            metadata['target_type'] = 'bike'
        if 'kph' in line.lower():
            metadata['target_speed'] = line.rsplit(':', 1)[1].strip()
            # this is not superfair, as longitudinal is run with speeds as well
            metadata['scenario'] = 'crossing'

        if ' m' in line.lower():
            metadata['target_distance'] = line.rsplit(':', 1)[1].strip()
            # fair to say that distances means crossing
            metadata['scenario'] = 'crossing'
    return metadata
