import pandas as pd
import numpy as np
import sys
sys.path.append('/mnt/proj/dsl-common-utils-da1/dsl_helpers')
import os
import pyspark
import yaml
import subprocess
import getpass
from cnx_spark_dsl3 import build_spark_session

class LocalSpark:
    # TODO
    def __init__(self) -> None:
        pass

class OnlineSpark:
    def __init__(self,config_path,size:str='test') -> None:
        self.path_dict = {
            'sut':{
                'root':'s3a://vasp-got-da/data/spark/rwup/sut/',
                'flc_obj':'s3a://vasp-got-da/data/spark/rwup/sut/record_id*flc*R5_object*',
                'vehicle':'s3a://vasp-got-da/data/spark/rwup/sut/record_id*vehicle*'
    
            },
            'avl_reference':{
                'root':'s3a://vasp-got-da/data/spark/rwup/avl_reference/',
                'ego':'s3a://vasp-got-da/data/spark/rwup/avl_reference/record_id*_ego_df*',
                'line':'s3a://vasp-got-da/data/spark/rwup/avl_reference/record_id*_line_df*',
                'object':'s3a://vasp-got-da/data/spark/rwup/avl_reference/record_id*_object_df*',
            }
        }
        self.config_path = config_path
        with open(self.config_path, 'r') as file: 
            self.config = yaml.safe_load(file)
        self.spark = build_spark_session(size = size,
                                        s3_access_key=self.config['S3_ACCESS_KEY_TEST_FLEET'],
                                         s3_secret_key=self.config['S3_SECRET_KEY_TEST_FLEET'])
    def loadParquet(self,dataset:str,table:str,record_id:str=''):
        parquet_path = self.path_dict[dataset][table]
        parquet_path = parquet_path.replace('record_id',record_id)
        df = self.spark.read.parquet(parquet_path)
        return df
    # def loadCSV(self,csv_file_path:str):
    #     df = self.spark.read.option("header", "true").csv(csv_file_path)
    #     return df
    def stop(self):
        self.spark.stop()


   