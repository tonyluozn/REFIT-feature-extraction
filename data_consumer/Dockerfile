FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY receive_data.py .

CMD ["python", "receive_data.py"]
