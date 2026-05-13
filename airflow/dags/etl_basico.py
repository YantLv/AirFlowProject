from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id= "etl_basico",
    start_date = datetime(2026,5,12),
    schedule = "@daily",
    catchup = False,
    default_args = default_args,
    description = "Pipeline ETL"
) as dag:

    def extraer():
        data = {
            "nombre": ["Daniel", "Yant", "Miguel", "Oscar", "Fernando", "Arturo", "Miriam"],
            "edad": [20, 23, 24, 30, 25, 26, 31 ]
        }
        df = pd.DataFrame(data)
        df.to_csv("/tmp/datos_alumnos.csv", index = False)
        print("Datos extraidos y guardados correctamente.")

    def transformar():
        df = pd.read_csv("/tmp/datos_alumnos.csv")
        df["edad"] = df["edad"] + 2
        df["mayor edad"] = df["edad"] >= 30
        df.to_csv("/tmp/datos_transformados.csv", index = False)
        print("Datos transformados")

    def cargar():
        df = pd.read_csv("/tmp/datos_transformados.csv")
        print("Datos finales")
        print(df)

    tarea_extraer = PythonOperator(
        task_id = "extraer_datos",
        python_callable = extraer
    )

    tarea_transformar = PythonOperator(
        task_id = "transformar_datos",
        python_callable = transformar
    )

    tarea_cargar = PythonOperator(
        task_id = "cargar_datos",
        python_callable = cargar
    )

    tarea_inicio = EmptyOperator(task_id = "INICIO_ETL")
    tarea_fin = EmptyOperator(task_id = "FIN_ETL")

tarea_inicio >> tarea_extraer >> tarea_transformar >> tarea_cargar >> tarea_fin