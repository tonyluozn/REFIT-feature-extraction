create table refit_raw_sensor_data
(
    projectGuid STRING,
    sensorId    STRING,
    timestamp STRING,
    doubles     STRING,
) with ( 'connector' = 'kafka',
      'topic' = 'refit.inference.raw.data',
      'properties.bootstrap.servers' = 'refit-kafka:9092',
      'properties.group.id' = 'testGroup-source',
      'format' = 'json',
      'scan.startup.mode' = 'earliest-offset')