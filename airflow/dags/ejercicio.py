from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import time
from airflow.utils.task_group import TaskGroup


# Funciones del pipeline
def extraer():
    datos = {
        "Productos" : ["Leche", "Huevos", "Arroz", "Harina de maiz", "Jugo de naranja", "Shampoo"],
        "Precio" : [32, 27, 25, 20, 32, 65]
    }

    df = pd.DataFrame(datos)
    df.to_csv("/tmp/datos_productos.csv", index=False)
    print("Datos extraídos correctamente.")
    

def transformar_descuento():
    df = pd.read_csv("/tmp/datos_productos.csv")
    df["Precio_descuento"] = df["Precio"] * 0.91
    df.to_csv("/tmp/datos_productos_transformados.csv", index = False)
    print(df)
    print("Datos transformados correctamente.")

def promedio_datos():
    df = pd.read_csv("/tmp/datos_productos.csv")
    promedio = df["Precio"].mean()
    print(f"Precio promedio de productos: {promedio}")

def reporte():
    df = pd.read_csv("/tmp/datos_productos.csv")
    promedio = df["Precio"].mean()
    df = df[df["Precio"] > promedio]
    print("Productos más caros que el promedio: ")
    print(df)
    df.to_csv("/tmp/datos_productos_caros.csv", index = False)
    print("Datos guardados correctamente.")

def cargar():
    df = pd.read_csv("/tmp/datos_productos.csv")
    df.to_csv("/tmp/datos_finales.csv", index = False)
    print("Datos finales cargados correctamente.")
# Argumentos default del dag

default_args = {
    "retries" : 3,
    "retry_delay" : timedelta(seconds = 10)
}

# Definicion del DAG
with DAG(
    dag_id = "ejercicio",
    start_date = datetime(2026,5,12),
    schedule = "@daily",
    catchup = False,
    description = "Ejercicio pipeline de productos"
) as dag:

    inicio = EmptyOperator(task_id = "Inicio")
    fin = EmptyOperator(task_id = "Fin")

    tarea_extraer = PythonOperator(
        task_id = "extraer_datos",
        python_callable = extraer
    )

    with TaskGroup("grupo_reportes") as grupo_reportes:
        tarea_promedio = PythonOperator(
        task_id = "promedio_datos",
        python_callable = promedio_datos
        )
        
        tarea_reporte = PythonOperator(
        task_id = "generar_reporte",
        python_callable = reporte
        )
    
    tarea_transformar = PythonOperator(
            task_id = "transformar_datos_descuento",
            python_callable = transformar_descuento
        )

    tarea_cargar = PythonOperator(
        task_id = "cargar_datos",
        python_callable = cargar
    )

inicio >> tarea_extraer >> tarea_cargar >> fin
tarea_extraer >> grupo_reportes
tarea_extraer >> tarea_transformar 