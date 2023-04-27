from pyflink.common.serialization import JsonDeserializationSchema, JsonSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer

class Main:
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.resource_file_name = "/inference.yaml"
        
        # Load configuration settings
        # In the Scala code, it uses a custom `ConfigFactory` class
        # You need to implement similar logic to load the configuration in Python
        kafka_settings = self.load_kafka_settings(self.resource_file_name)
        checkpoint_interval = 1000 * 60
        
        kafka_config = {
            "bootstrap.servers": kafka_settings["host"],
            "auto.offset.reset": "latest",
            "group.id": "refit.inference"
        }
        
        self.env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        self.env.get_checkpoint_config().set_checkpoint_interval(checkpoint_interval)

        raw_sensor_data_schema = JsonDeserializationSchema.builder()\
            .type_info(Types.POJO(SensorData))\
            .build()

        sensor_data_schema = JsonDeserializationSchema.builder()\
            .type_info(Types.POJO(SensorDataJson))\
            .build()

        raw_sensor_data_source = FlinkKafkaConsumer(
            kafka_settings["topics"]["data"],
            raw_sensor_data_schema,
            properties=kafka_config
        )

        sensor_data_source = FlinkKafkaConsumer(
            kafka_settings["topics"]["sensorData"],
            sensor_data_schema,
            properties=kafka_config
        )

        raw_sensor_data = self.env.add_source(raw_sensor_data_source)
        sensor_data = self.env.add_source(sensor_data_source)\
            .map(StaticDataMapper())\
            .key_by(lambda value: value.project_guid)

        raw_sensor_data_sink = FlinkKafkaProducer(
            kafka_settings["topics"]["rawSensorData"],
            JsonSerializationSchema.builder().type_info(Types.POJO(SensorData)).build(),
            kafka_config
        )

        raw_sensor_data.add_sink(raw_sensor_data_sink)
        self.env.execute("CDL IoT - Inference")

    # Implement this method to load Kafka settings from the given file
    def load_kafka_settings(self, resource_file_name: str) -> dict:
        pass

if __name__ == "__main__":
    Main()
