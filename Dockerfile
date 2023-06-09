FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data/raw /opt/prefect/data/raw
RUN mkdir -p /opt/prefect/data/gcs
RUN mkdir -p /opt/prefect/data/extract


