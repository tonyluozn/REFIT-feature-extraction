import json
from pandas import Series
from pyflink.table import DataTypes
from pyflink.table.udf import udf

@udf(input_types=[DataTypes.STRING(), DataTypes.STRING(),
                  DataTypes.STRING(), DataTypes.STRING()],
     result_type=DataTypes.STRING(), udf_type='pandas')
def doubles(project_guid: Series,
            sensor_id: Series,
            timestamp: Series,
            doubles: Series) -> Series:
    doubles_dict = doubles.apply(json.loads)
    # Perform feature extraction on the doubles_dict Series
    # ...
    features = [...]  # Replace with the actual extracted features
    return json.dumps(features)
