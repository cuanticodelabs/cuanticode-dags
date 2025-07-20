from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import instaloader
import csv
import os

# Configuración
USERNAME = "bardelobos.cl"  # Cuenta pública
OUTPUT_PATH = f"/home/cuanticodelabs/airflow/dags/{USERNAME}_metrics.csv"

def fetch_instagram_metrics(username=USERNAME):
    """Extrae métricas básicas de una cuenta pública de Instagram y las guarda en un CSV."""
    loader = instaloader.Instaloader()
    profile = instaloader.Profile.from_username(loader.context, username)

    metrics = {
        "username": profile.username,
        "followers": profile.followers,
        "followees": profile.followees,
        "posts": profile.mediacount,
        "biography": profile.biography,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    file_exists = os.path.isfile(OUTPUT_PATH)
    with open(OUTPUT_PATH, mode="a", newline='', encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=metrics.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(metrics)

    print(f"Datos guardados en {OUTPUT_PATH}: {metrics}")


# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 20),
    'email': ['hectogonzalezb@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG sin usar 'with'
dag = DAG(
    'instagram_daily_report_dag',
    default_args=default_args,
    description='DAG diario que extrae métricas de Instagram y envía reporte por correo',
    schedule_interval='@daily',  # Ejecución diaria
    catchup=False
)

# Tarea para obtener métricas
task_fetch_metrics = PythonOperator(
    task_id='fetch_instagram_metrics',
    python_callable=fetch_instagram_metrics,
    dag=dag
)

# Tarea para enviar correo con el CSV (HTML mejorado)
task_send_email = EmailOperator(
    task_id='send_instagram_report',
    to='hectogonzalezb@gmail.com',
    subject='📊 Reporte Diario Instagram - bardelobos.cl',
    html_content="""
    <div style="font-family: Arial, sans-serif; background: #f7f7f7; padding: 30px;">
      <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 10px; box-shadow: 0 2px 8px #e0e0e0; padding: 30px;">
        <h2 style="color: #405de6; text-align: center; margin-top: 0;">
          📈 Reporte Diario de Instagram
        </h2>
        <p style="font-size: 16px; color: #333;">
          ¡Hola!<br>
          Se adjuntan las métricas más recientes de la cuenta <b style="color: #405de6;">bardelobos.cl</b>.
        </p>
        <div style="margin: 30px 0; text-align: center;">
          <img src="https://upload.wikimedia.org/wikipedia/commons/a/a5/Instagram_icon.png" alt="Instagram" width="60" style="border-radius: 12px;">
        </div>
        <p style="font-size: 15px; color: #555;">
          El archivo adjunto contiene los datos de seguidores, seguidos, publicaciones y biografía.<br>
          <b>¡Que tengas un gran día!</b>
        </p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
        <footer style="font-size: 13px; color: #aaa; text-align: center;">
          Reporte automático generado por Airflow | bardelobos.cl
        </footer>
      </div>
    </div>
    """,
    files=[OUTPUT_PATH],
    dag=dag
)

task_fetch_metrics >> task_send_email
