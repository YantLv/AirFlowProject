from airflow import DAG
from airflow.operators.python import PythonOperator #ejecuta codigo python
from datetime import datetime
from airflow.operators.bash import BashOperator # ejecuta comandos Linux
from datetime import timedelta
from airflow.operators.empty import EmptyOperator # operador vacio

def saludar():
    return print("Hola Airflow")
def despedida():
    return print("Adios Airflow")
def presentar():
    return print("Mi nombre es Yant")
with DAG(
    dag_id = "hola_airflow",
    start_date = datetime(2026,5,12),
    schedule = "@daily",
    catchup = False
) as dag:

    tarea_saludo = PythonOperator(
        task_id = "decir_hola", 
        python_callable = saludar
    )

    tarea_despedida = PythonOperator(
        task_id = "decir_adios",
        python_callable = despedida
    )
    tarea_presentar = PythonOperator(
        task_id = "presentarse",
        python_callable = presentar
    )

    tarea_fecha = BashOperator(
        task_id = "mostrar_fecha",
        bash_command = "date"
    )

    tarea_mensaje = BashOperator(
        task_id = "mostrar_mensaje",
        bash_command = "echo 'Estoy aprendiendo Airflow'"
    )

    tarea_archivos = BashOperator(
        task_id = "mostrar_archivos",
        bash_command = "pwd"
    )
    # intenta varias veces si falla
    tarea_reintentos = BashOperator(
        task_id = "reintentos",
        bash_command = "pwd",
        retries = 3,
        retry_delay = timedelta(seconds = 10)
    )

    tarea_inicio = EmptyOperator(task_id = "INICIO")
    tarea_fin = EmptyOperator(task_id = "FIN")

tarea_inicio >> tarea_despedida >> tarea_presentar >> tarea_saludo >> tarea_fecha >> tarea_mensaje >> tarea_archivos >> tarea_reintentos >> tarea_fin

