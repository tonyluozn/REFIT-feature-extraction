# pipeline.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from feature_extraction.feature_extractor import doubles

class RefitFeatureEnrichment():
    def __init__(self):
        self.settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        self.table_env = StreamTableEnvironment.create(self.env, environment_settings=self.settings)
        self.table_env.add_python_file('feature_extractors')

        source_table = open('feature_extractors/source.sql', 'r').read()
        sink_table = open('feature_extractors/sink.sql', 'r').read()

        self.table_env.execute_sql(source_table)
        self.table_env.execute_sql(sink_table)

    def run(self):
        self.table_env.register_function("doubles", doubles)

        self.table_env.execute_sql("""
        INSERT INTO refit_sensor_data
        SELECT projectGuid, sensorId, timestamp, doubles(projectGuid, sensorId, timestamp, doubles, strings, integers, labels)
        FROM refit_raw_sensor_data
        """).get_job_client().get_job_execution_result().result()

        self.env.execute("CDL IoT - Feature Extraction")

RefitFeatureEnrichment().run()
