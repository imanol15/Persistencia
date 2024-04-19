from datetime import datetime, timedelta
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración del cliente de InfluxDB
url = "http://localhost:8086"
token = "_ox9GvfULj4I21TZLjSBQoqhAS9dEitgStrLvec9vh_pKDFpAb_qkPH9DBi-1HxMWpWeSKV0lfLa9ZpsB1AYdw=="
org = "Deusto"
bucket = "Deusto"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Leer el dataset
data_path = 'T1.csv'
data = pd.read_csv(data_path)

# Calcular la hora inicial
initial_time = datetime.utcnow()

# Insertar datos en InfluxDB
for index, row in data.iterrows():
    time_offset = timedelta(seconds=5 * index)
    current_time = initial_time - time_offset
    point = (
        Point("turbina")
        .tag("unidad", "T1")
        .field("LV_ActivePower_kW", row['LV ActivePower (kW)'])
        .field("Wind_Speed_m_s", row['Wind Speed (m/s)'])
        .field("Theoretical_Power_Curve_KWh", row['Theoretical_Power_Curve (KWh)'])
        .field("Wind_Direction", row['Wind Direction (°)'])
        .time(current_time)
    )
    write_api.write(bucket=bucket, org=org, record=point)

# Consulta para recuperar datos promediados
query = 'from(bucket: "Deusto") |> range(start: -1d) |> filter(fn: (r) => r._measurement == "turbina") |> mean()'
tables = client.query_api().query(query, org=org)
for table in tables:
    for record in table.records:
        logging.info(record.values)

# Cerrar cliente de InfluxDB después de realizar todas las operaciones
client.close()