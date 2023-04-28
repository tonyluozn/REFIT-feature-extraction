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
from pyflink.table import DataTypes
from pyflink.table.udf import udf
from pyflink.table import expressions as expr
from pyflink.common.serialization import SerializationSchema

class RowToStringSerializationSchema(SerializationSchema):
    def serialize(self, value):
        return str(value).encode("utf-8")

@udf(input_types=[DataTypes.STRING()],
     result_type=DataTypes.STRING(),
     udf_type='pandas')
def transform(input_data: Series) -> Series:
    return input_data



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
t_env = StreamTableEnvironment.create(env)

# env.add_jars("file:///opt/flink_job/libs/flink-connector-kafka_2.12-1.14.3.jar")
# Set the path to the Flink Kafka connector JAR file
# flink_kafka_connector_jar = "file:///opt/flink_job/libs/flink-connector-kafka_2.12-1.14.3.jar"
# Configure the PyFlink gateway
# os.environ["PYFLINK_GATEWAY_OPTS"] = f"-Dpipeline.jars={flink_kafka_connector_jar}"


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
t_env.create_temporary_function("transform", transform)
output_table = input_table.select(expr.call("transform", input_table.f0).alias("result"))

# Set up the Kafka producer
producer_props = {
    'bootstrap.servers': 'kafka:9092',
    'transaction.timeout.ms': '10000'
}
producer = FlinkKafkaProducer(SINK_KAFKA_TOPIC, RowToStringSerializationSchema(), producer_props)

# Write to Kafka
output_stream = t_env.to_data_stream(output_table.select(col("result")))
output_stream.add_sink(producer)

# Submit the job
env.execute("Feature Extraction")
