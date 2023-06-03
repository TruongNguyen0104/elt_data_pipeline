FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -rrequirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY code /opt/prefect/code
RUN mkdir -p /opt/prefect/data/crime