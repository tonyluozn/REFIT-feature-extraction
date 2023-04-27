from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.window import Time, SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.assigners import BoundedOutOfOrdernessTimestampExtractor
from pyflink.table import StreamTableEnvironment
import os

class TransformWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        # Your transformation function goes here
        enriched_data = elements[0]  # Replace with your transformation logic using the 1-hour window data (elements)
        out.collect(enriched_data)

class Strategy(BoundedOutOfOrdernessTimestampExtractor):
    def __init__(self):
        super().__init__(Time.seconds(10))  # Replace 10 with the maximum allowed out-of-orderness (in seconds)

    def extract_timestamp(self, element):
        # Replace this with your actual timestamp extraction logic
        return int(element.split(',')[0]) * 1000  # Assuming the first field in the input data is the timestamp (in seconds)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
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

# Assign timestamps and watermarks
input_stream = input_stream.assign_timestamps_and_watermarks(
    Strategy()
)

# Apply transformation with 1-hour window
output_stream = input_stream \
    .key_by(lambda x: 1) \
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(1))) \
    .process(TransformWindowFunction(), output_type=Types.STRING())

# Set up the Kafka producer
producer_props = {
    'bootstrap.servers': 'kafka:9092',
    'transaction.timeout.ms': '10000'
}
producer = FlinkKafkaProducer(SINK_KAFKA_TOPIC, SimpleStringSchema(), producer_props)

# Write to Kafka
output_stream.add_sink(producer)

# Submit the job
env.execute("Feature Extraction")
