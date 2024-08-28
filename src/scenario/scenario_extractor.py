from src.scenario.data_loader import DataLoader
from src.scenario.scenario_filter import ScenarioFilter
from pyspark.sql import functions as F
from scripts.dataloader import OnlineSpark

def get_scenarios():
    spark_config_path = '/home/a494189/vasp-got-da/scripts/team_data_analytics/Lingbin_Bokuan/config.yaml'
    session = OnlineSpark(spark_config_path, size='large')

    config_path = '/home/a494189/vasp-got-da/scripts/team_data_analytics/Lingbin_Bokuan/Bokuan/src/scenario/config.yaml'
    data_loader = DataLoader(session)
    filters = ScenarioFilter(config_path)

    # Load data
    ego_df = data_loader.load_data('ego')
    ego_df = ego_df.withColumn("Velocity", F.sqrt(F.pow(ego_df.VelocityX, 2) + F.pow(ego_df.VelocityY, 2)))
    obj_columns = ["ptp_time","MessageNumber","ReferenceType","TrackingID","ExistConf","LifeTime","MeasurementState",\
                    "Length","Width","Height","PositionX","PositionY","PositionZ","Roll","Pitch","Yaw","VelocityX",\
                    "VelocityY","VelocityZ","AccelerationX","AccelerationY","AccelerationZ","ReferencePoint",\
                    "MovementState","ClassificationProbability","ObjType","ObjVehicleClassification","recording_id","vehicle"]
    obj_df = data_loader.load_data('object').dropDuplicates(obj_columns)
    filtered_obj_df = filters.filter_obj(obj_df)
    status_df = data_loader.load_status()
    
    ego_obj = filtered_obj_df.join(
    ego_df,
    (filtered_obj_df.ptp_time == ego_df.ptp_time)&(filtered_obj_df.recording_id == ego_df.recording_id),
    "inner").drop(ego_df.ptp_time, ego_df.VelocityX, ego_df.recording_id)
    num_obj = ego_obj.groupBy("recording_id", "ptp_time").agg(
                F.count("*").alias("count"),
                F.avg("VelocityX").alias("avg_v"),
                F.avg("Velocity").alias("ego_v"))

    # Filter operations
    lane_change_df = filters.filter_lane_change(status_df)
    emergency_brake_df = filters.filter_emergency_brake(status_df)
    turning_df = filters.filter_turning(status_df)
    congestion_df = filters.filter_congestion(num_obj)
    # print(lane_change_df.count(),emergency_brake_df.count(),turning_df.count(),congestion_df.count())
    # Combine status
    final_df = filters.combine_status(filtered_obj_df, lane_change_df, emergency_brake_df, turning_df, congestion_df)
    
    # final_df.show()

    return final_df

if __name__ == "__main__":
    get_scenarios()
