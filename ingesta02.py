import boto3
import pandas as pd
import mysql.connector
from io import StringIO

# Parámetros
nombreBucket = "flx-output-mysql"
archivo_csv_s3 = "data.csv"

# Datos de conexión a MySQL (cambia estos valores por los tuyos)
db_config = {
    'host': 'mysql',
    'user': 'admin',
    'password': 'utec',
    'database': 'utecbd',
    'port': 3306
}

# Nombre de la tabla destino
tabla = "tabla_utec"

def descargar_csv_desde_s3(bucket, key):
    s3 = boto3.client('s3')
    csv_obj = s3.get_object(Bucket=bucket, Key=key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    return csv_string

def cargar_datos_mysql(df, conexion, tabla):
    cursor = conexion.cursor()
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {tabla} ({cols}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))
    conexion.commit()
    cursor.close()

def main():
    try:
        # Descargar CSV desde S3
        csv_string = descargar_csv_desde_s3(nombreBucket, archivo_csv_s3)

        # Leer CSV a DataFrame
        df = pd.read_csv(StringIO(csv_string))

        # Conectar a MySQL
        conexion = mysql.connector.connect(**db_config)

        # Cargar datos en MySQL
        cargar_datos_mysql(df, conexion, tabla)

        conexion.close()
        print("Ingesta completada exitosamente.")

    except Exception as e:
        print("Error durante la ingesta:", e)

if __name__ == "__main__":
    main()
