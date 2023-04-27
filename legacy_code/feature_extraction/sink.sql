create table refit_sensor_data
(
    projectGuid STRING,
    sensorId    STRING,
    timestamp STRING,
    doubles     STRING,
) with ( 'connector' = 'kafka',
      'topic' = 'refit.inference.sensor.data',
      'properties.bootstrap.servers' = 'refit-kafka:9092',
      'properties.group.id' = 'testGroup',
      'format' = 'json',
      'scan.startup.mode' = 'earliest-offset')