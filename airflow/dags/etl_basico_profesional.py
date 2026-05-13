from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import time
from airflow.utils.task_group import TaskGroup
 
# -------------------------
# Funciones del pipeline
# -------------------------
 
def extraer():
    data = {
        "nombre": ["Daniel", "Yant", "Miguel", "Oscar", "Fernando", "Arturo", "Miriam"],
        "edad": [20, 23, 24, 30, 25, 26, 31]
    }
 
    df = pd.DataFrame(data)
    df.to_csv("/tmp/datos_alumnos.csv", index=False)
    print("Datos extraídos correctamente.")
 
 
def transformar():
    df = pd.read_csv("/tmp/datos_alumnos.csv")
    df["edad"] = df["edad"] + 2
    df.to_csv("/tmp/datos_alumnos_transformados.csv", index=False)
    print("Datos transformados correctamente.")
 
def transformar_edad_treinta():
    df = pd.read_csv("/tmp/datos_alumnos.csv")
    df["edad"] = df["edad"] + 2
    df["mayor_edad"] = df["edad"] >= 30
    df.to_csv("/tmp/datos_alumnos_transformados_treinta.csv", index=False)
    print("Datos transformados correctamente.")

def generar_reporte():
    df = pd.read_csv("/tmp/datos_noexisten.csv")
    promedio = df["edad"].mean()
    print(f"Promedio edad: {promedio}")

def cargar():
    df = pd.read_csv("/tmp/datos_alumnos_transformados_treinta.csv")
 
    print("Datos finales:")
    print(df)
 
def paralelo():
    time.sleep(10)
    print("Paralelo")
# -------------------------
# Configuración del DAG
# -------------------------
 
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}
 
# ------------------------
# Definición del DAG
# ------------------------
with DAG(
    dag_id="etl_basico_profesional",
    start_date=datetime(2026, 5, 12),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="Pipeline ETL básico con estructura profesional"
) as dag:
 
    inicio = EmptyOperator(task_id="inicio")
 
    tarea_extraer = PythonOperator(
        task_id="extraer_datos",
        python_callable=extraer
    )
 
    tarea_reporte = PythonOperator(
        task_id = "generar_reporte",
        python_callable = generar_reporte
    )

    tarea_cargar = PythonOperator(
        task_id="cargar_datos",
        python_callable=cargar
    )
    
    tarea_paralelo = PythonOperator(
        task_id = "paralelo",
        python_callable = paralelo
    )
    fin = EmptyOperator(task_id="fin")

    with TaskGroup("grupo_transformaciones") as grupo_transformaciones:
        tarea_transformar = PythonOperator(
            task_id="transformar_datos",
            python_callable=transformar
        )
        tarea_transformar_treinta = PythonOperator(
            task_id = "transformar_edad_treinta",
            python_callable = transformar_edad_treinta
        )

        tarea_transformar >> tarea_transformar_treinta
 
    #inicio >> tarea_extraer  >> [tarea_transformar, tarea_transformar_treinta, tarea_reporte] >> tarea_cargar >> fin
    inicio >> tarea_extraer
    tarea_extraer >> grupo_transformaciones >> tarea_cargar >> fin
    tarea_extraer >> tarea_reporte
    tarea_extraer >> tarea_paralelo 
