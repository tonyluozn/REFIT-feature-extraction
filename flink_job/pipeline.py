from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment
import os

def transform(data):
    # Your transformation function goes here
    return data

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
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
input_table = t_env.from_data_stream(input_stream)

# Apply transformation
output_table = input_table.select("transform(value)").alias("result")
t_env.register_function("transform", transform)

# Set up the Kafka producer
producer_props = {
    'bootstrap.servers': 'kafka:9092',
    'transaction.timeout.ms': '10000'
}
producer = FlinkKafkaProducer(SINK_KAFKA_TOPIC, SimpleStringSchema(), producer_props)

# Write to Kafka
output_stream = output_table.select("result")
output_stream.add_sink(producer)

# Submit the job
env.execute("My Flink job")
