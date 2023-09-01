# REFIT Real-time Feature Extraction
This project includes a functional demo of a robust ML real-time data processing pipeline, from data ingestion through feature engineering, using Kafka for message queuing and Apache Flink for stream processing and job management. This is an integral component of the REFIT project at the Center for Deep Learning. For more information, please visit [REFIT](https://www.mccormick.northwestern.edu/research/deep-learning/projects/refit/)

The goal is to enhance the feature engineering phase of REFIT through real-time computation of relevant features in Flink based on a sliding window of historical data. 

# System Overview
We use Docker Compose configuration to set up a multi-service system for data processing and streaming using various components. The system is designed to handle data ingestion, data processing, and messaging via Kafka, as well as job management with Apache Flink. Below is a high-level overview of the services and their roles within the system:

## Data Ingestion
Custom Build: ./data_ingestion 

Dependencies: Depends on Kafka.

Function: This service serves as the producer of real-time streaming data and ingests raw data into the Kafka topic `refit.raw.data`.

## Feature Engineering
Custom Build: ./flink_job

Dependencies: Depends on Kafka, Job Manager, and Task Manager.

Function: Executes an Apache Flink job for feature engineering, reading from the topic `refit.raw.data` in Kafka and writing results to `refit.feature.data`. In this project, the computed feature is the average of the temperature data in a sliding window of 30 seconds. More concretely, for any incoming data with timestamp t, we compute the average of all data in the window [t - 30s, t].


## Data Consumer
Custom Build: ./data_consumer

Dependencies: Depends on Kafka and Data Ingestion.

Function: This service consumes data from the Kafka topic `refit.feature.data` and outputs the data in the log.

# Instructions to Run

To run the system, ensure that you have `docker` and `docker-compose` correctly installed. Then, execute the following command from the root directory:

`docker compose up --build`.

To monitor the system's logs, real-time ingested data can be found in the `data_ingestion` container's logs, while feature-enriched results are available in the `data_consumer` container's logs.
