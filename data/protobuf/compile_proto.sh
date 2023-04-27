python -m grpc_tools.protoc -I. --python_out=. raw_data.proto
python -m grpc_tools.protoc -I. --python_out=. feature_data.proto