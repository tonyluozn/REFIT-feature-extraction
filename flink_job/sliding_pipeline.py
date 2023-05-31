from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment
from pyflink.common import Configuration
import os
import json
from pandas import Series
import logging
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common import Types
from pyflink.common.time import Time
from typing import List
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common import WatermarkStrategy
from typing import Iterable
from datetime import datetime
from pyflink.datastream.state import ListState, ListStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

class CustomProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.event_queue = None

    def open(self, runtime_context: RuntimeContext):
        self.event_queue = runtime_context.get_list_state(ListStateDescriptor('event_queue', Types.STRING()))

    def process_element(self, value, ctx):
        # add the current event to the queue
        self.event_queue.add(value)

        # create a window of events in the past 30 s
        window_start = ctx.timestamp() - 0.5 * 60 * 1000
        events_in_window = [event for event in self.event_queue.get() if self.extract_timestamp(event) >= window_start]

        # process the window
        input_data_list = [json.loads(event) for event in events_in_window]
        temperatures = [data["doubles"]["temperature"] for data in input_data_list]
        humidities = [data["doubles"]["humidity"] for data in input_data_list]

        average_temperature = round(sum(temperatures) / len(temperatures),2) if temperatures else 0
        average_humidity = round(sum(humidities) / len(humidities),2) if humidities else 0

        result = input_data_list[-1] if input_data_list else {}
        result["features"] = {
            "average_temperature": average_temperature,
            "average_humidity": average_humidity
        }
        result.pop("doubles", None)
        # output the result
        result = json.dumps(result)
        # out.collect(result)

        # remove the events outside of the window from the queue
        self.event_queue.clear()
        for event in events_in_window:
            self.event_queue.add(event)
        yield result

    def extract_timestamp(self, value):
        data = json.loads(value)
        timestamp_str = data['timestamp']
        dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        unix_timestamp = int(dt.timestamp() * 1000)
        return unix_timestamp


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        data = json.loads(value)
        timestamp_str = data['timestamp']
        dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        unix_timestamp = int(dt.timestamp() * 1000)
        return unix_timestamp

watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
    .with_timestamp_assigner(MyTimestampAssigner())



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
    .process(CustomProcessFunction(), Types.STRING()) \
    .add_sink(producer)

# Submit the job
logging.info("Submitting the job")
env.execute("Feature Extraction")
