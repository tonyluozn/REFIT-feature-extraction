from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment
from pyflink.common import Configuration
from pyflink.table import DataTypes
from pyflink.table.expressions import col 
from pyflink.table.udf import ScalarFunction
import os
import json
from pandas import Series
import logging
from pyflink.table import DataTypes
from pyflink.table.udf import udf
from pyflink.table import expressions as expr
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SerializationSchema
from pyflink.common import Types
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from typing import List
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TimeWindow
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import SlidingEventTimeWindows
from typing import Iterable
from datetime import datetime
from pyflink.datastream.window import WindowAssigner, TimeWindow, EventTimeTrigger, TimeWindowSerializer

# import typing

class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        data = json.loads(value)
        timestamp_str = data['timestamp']
        dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        unix_timestamp = int(dt.timestamp() * 1000)
        return unix_timestamp

watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
    .with_timestamp_assigner(MyTimestampAssigner())

class CustomEventTimeWindowAssigner(WindowAssigner):
    def __init__(self, window_size_seconds):
        self.window_size_seconds = window_size_seconds * 1000  # Convert to milliseconds

    def assign_windows(self, element, timestamp: int, ctx):
        start = timestamp - self.window_size_seconds
        end = timestamp
        return [TimeWindow(start, end)]

    def get_default_trigger(self, env):
        return EventTimeTrigger.create()

    def get_window_serializer(self):
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return True


class TransformProcessWindowFunction(ProcessWindowFunction):
    def __init__(self):
        super(TransformProcessWindowFunction, self).__init__()

    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[str]) -> Iterable[str]:
        input_data_list = [json.loads(element) for element in elements]
        temperatures = [data["doubles"]["temperature"] for data in input_data_list]
        humidities = [data["doubles"]["humidity"] for data in input_data_list]

        average_temperature = sum(temperatures) / len(temperatures) if temperatures else 0
        average_humidity = sum(humidities) / len(humidities) if humidities else 0

        result = input_data_list[-1] if input_data_list else {}
        result["features"] = {
            "average_temperature": average_temperature,
            "average_humidity": average_humidity
        }
        result.pop("doubles", None)

        return [json.dumps(result)]


# Set the Flink JobManager address and port
jobmanager_address = os.environ['JOBMANAGER_ADDRESS']
jobmanager_port = os.environ['JOBMANAGER_PORT']

# Configure the PyFlink gateway
os.environ["PYFLINK_GATEWAY_OPTS"] = f"-Djobmanager.rpc.address={jobmanager_address} -Djobmanager.rpc.port={jobmanager_port}"


# Add this line before creating the StreamExecutionEnvironment
configuration = Configuration()
configuration.set_string("pipeline.jars", "file:///opt/flink_job/libs/flink-connector-kafka_2.12-1.14.3.jar;file:///opt/flink_job/libs/kafka-clients-2.8.1.jar")

# Get the StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment(configuration)
env.set_parallelism(1)
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
    3,  # Number of restart attempts
    10000  # Delay between attempts (in milliseconds)
))
t_env = StreamTableEnvironment.create(env)

# Set up the Kafka consumer
consumer_props = {
    'bootstrap.servers': os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    'group.id': 'feature_extraction',
    'auto.offset.reset': 'earliest'
}
SOURCE_KAFKA_TOPIC = os.environ["KAFKA_SOURCE_TOPIC"]
SINK_KAFKA_TOPIC = os.environ["KAFKA_SINK_TOPIC"]
consumer = FlinkKafkaConsumer(SOURCE_KAFKA_TOPIC, SimpleStringSchema(), consumer_props)

# Read from Kafka
input_stream = env.add_source(consumer)

# Set up the Kafka producer
producer_props = {
    'bootstrap.servers': 'kafka:9092',
    'transaction.timeout.ms': '10000'
}

# type_info = Types.ROW([Types.STRING()])
# serialization_schema = JsonRowSerializationSchema.Builder().with_type_info(type_info).build()
producer = FlinkKafkaProducer(SINK_KAFKA_TOPIC, SimpleStringSchema(), producer_props)

# Write to Kafka
# Apply transformation
input_stream.assign_timestamps_and_watermarks(watermark_strategy) \
    .key_by(lambda x: x[0], key_type=Types.STRING()) \
    .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5))) \
    .process(TransformProcessWindowFunction(), Types.STRING()) \
    .add_sink(producer)

# Submit the job
logging.info("Submitting the job")
env.execute("Feature Extraction")
