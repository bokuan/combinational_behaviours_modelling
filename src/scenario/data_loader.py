from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd

class DataLoader:
    def __init__(self, session):
        self.session = session

    def load_data(self, table_name: str) -> DataFrame:
        data_df = self.session.loadParquet('avl_reference', table_name)
        return self.highway_filter(data_df)

    def load_status(self) -> DataFrame:
        useful_columns = [
        "ptp_time_rounded","recording_id","vehicle","BrakePedalPosition", "BrakeSwitch", 
        "BrakeLight_rqst", "LateralAcceleration", "LongitudinalAcceleration", "VehicleDirSAE_VehSpeed",
        "WheelBasedVehicleSpeed", "LaneChangeStatus", "YawRate", "SteeringWheelAngle"]       
        status_df = self.session.loadParquet('sut', 'vehicle')\
                    .select(*useful_columns)\
                    .withColumnRenamed("ptp_time_rounded", "ptp_time")
        return self.highway_filter(status_df)
    
    def highway_filter(self, df: DataFrame) -> DataFrame:
        csv_file_path = '/home/a494189/vasp-got-da/scripts/team_data_analytics/Lingbin_Bokuan/Bokuan/data/highway.csv'
        csv_df = pd.read_csv(csv_file_path)
        highway_df = self.session.spark.createDataFrame(csv_df)

        highway_df = highway_df.withColumn("start_time", F.col("start_time").cast("double"))
        highway_df = highway_df.withColumn("end_time", F.col("end_time").cast("double"))

        filtered_df = df.join(
            highway_df,
            (df.recording_id == highway_df.recording_id) &
            (F.expr(f"ptp_time >= start_time") & F.expr(f"ptp_time <= end_time")),
            "inner")\
            .drop(highway_df.recording_id,highway_df.start_time,highway_df.end_time)
        filtered_df = filtered_df.withColumn("ptp_time", F.round(filtered_df["ptp_time"], 1))

        return filtered_df
