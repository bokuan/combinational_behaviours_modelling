import yaml
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

class ScenarioFilter:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def filter_obj(self, obj_df: DataFrame) -> DataFrame:
        thresholds = self.config['thresholds']
        filtered_obj = obj_df.filter(
            (F.abs(obj_df.VelocityX) < thresholds['velocity_x_max']) &
            (F.abs(obj_df.VelocityY) < thresholds['velocity_y_max']) &
            (F.abs(obj_df.VelocityZ) < thresholds['velocity_z_max']) &
            (obj_df.VelocityX > thresholds['velocity_x_min']) &
            (obj_df.PositionY < thresholds['position_y_max']) &
            (obj_df.PositionY > thresholds['position_y_min']) &
            (obj_df.PositionX < thresholds['position_x_max'])
        )
        return filtered_obj.dropDuplicates()

    def filter_lane_change(self, status_df: DataFrame) -> DataFrame:
        config = self.config['lane_change']
        return status_df.filter(
            (status_df['LaneChangeStatus'] != 0) &
            (status_df['VehicleDirSAE_VehSpeed'] > config['vehicle_speed_min'])
        ).select("ptp_time","recording_id").withColumn("status", F.lit(1)).dropDuplicates(["ptp_time", "recording_id"])

    def filter_emergency_brake(self, status_df: DataFrame) -> DataFrame:
        config = self.config['emergency_brake']
        return status_df.filter(
            (status_df['LaneChangeStatus'] == 0) &
            (status_df['BrakeSwitch'] != 0) &
            (status_df['BrakePedalPosition'] > config['brake_pedal_position_min']) &
            (status_df['VehicleDirSAE_VehSpeed'] > config['vehicle_speed_min']) &
            (status_df['BrakeLight_rqst'] != 0)
        ).select("ptp_time","recording_id").withColumn("status", F.lit(2)).dropDuplicates(["ptp_time", "recording_id"])

    def filter_turning(self, status_df: DataFrame) -> DataFrame:
        config = self.config['turning']
        return status_df.filter(
            (status_df['LaneChangeStatus'] == 0) &
            (F.abs(status_df['SteeringWheelAngle']) > config['steering_wheel_angle_min']) &
            (status_df['VehicleDirSAE_VehSpeed'] > config['vehicle_speed_min'])
        ).select("ptp_time","recording_id").withColumn("status", F.lit(3)).dropDuplicates(["ptp_time", "recording_id"])

    def filter_congestion(self, num_obj_df: DataFrame) -> DataFrame:
        config = self.config['congestion']
        return num_obj_df.filter(
            (num_obj_df['count'] > config['object_count_min']) &
            (num_obj_df['avg_v'] < config['avg_velocity_max']) &
            (num_obj_df['ego_v'] < config['ego_velocity_max'])
        ).select("ptp_time","recording_id").withColumn("status", F.lit(4)).dropDuplicates(["ptp_time", "recording_id"])

    def combine_status(self, filtered_obj: DataFrame, *status_dfs) -> DataFrame:
        new_df = filtered_obj.withColumn("status", F.lit(0))
        for status_df in status_dfs:
            new_df = new_df.join(
                status_df.withColumnRenamed("status", "temp_status"),
                on=["ptp_time", "recording_id"], how="left"
            ).withColumn("status", F.coalesce(F.col("temp_status"), F.col("status"))) \
             .drop("temp_status")
        return new_df
