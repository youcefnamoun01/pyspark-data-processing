from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from utils.processing_functions import clean_str, remove_duplicates, handle_nulls, load_data
from utils.eda_functions import (
    most_and_least_ordered_products, order_distribution_by_product, most_frequent_product_pairs,
    most_and_least_frequent_aisle_pairs, hourly_order_distribution, get_top_users, get_user_distribution
)

# ------------------ DAG Arguments ------------------
default_args = {
    'owner': 'youcef',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pyspark_etl_eda_xcom',
    default_args=default_args,
    description='ETL PySpark et analyse exploratoire avec passage de chemins via XCom',
    schedule_interval=None,
)

# ------------------ Fonctions des tâches ------------------
def init_spark(**kwargs):
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    spark = SparkSession.builder \
        .appName("PySparkETL") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return None  # SparkSession sera recréée dans chaque tâche si besoin

def load_and_clean_data(**kwargs):
    spark = SparkSession.builder.getOrCreate()

    orders = load_data(spark, "orders")
    order_products_prior = load_data(spark, "order_products__prior")
    products = load_data(spark, "products")
    aisles = load_data(spark , "aisles")
    departments = load_data(spark, "departments")

    # Nettoyage chaînes
    products = clean_str(products, "product_name")
    aisles = clean_str(aisles, "aisle")
    departments = clean_str(departments, "department")

    # Suppression des doublons
    order_products_prior = remove_duplicates(order_products_prior, ["order_id", "product_id"])
    products = remove_duplicates(products, ["product_id"])
    orders = remove_duplicates(orders, ["order_id"])
    aisles = remove_duplicates(aisles,["aisle_id"])
    departments = remove_duplicates(departments, ["department_id"])

    # Gestion des nulls
    order_products_prior = handle_nulls(order_products_prior)
    products = handle_nulls(products)
    orders = handle_nulls(orders)
    aisles = handle_nulls(aisles)
    departments = handle_nulls(departments)

    # Sauvegarde temporaire locale
    tmp_dir = "/tmp/airflow_pyspark"
    os.makedirs(tmp_dir, exist_ok=True)

    orders_path = os.path.join(tmp_dir, "orders.parquet")
    order_products_prior_path = os.path.join(tmp_dir, "order_products_prior.parquet")
    products_path = os.path.join(tmp_dir, "products.parquet")
    aisles_path = os.path.join(tmp_dir, "aisles.parquet")
    departments_path = os.path.join(tmp_dir, "departments.parquet")

    orders.write.mode("overwrite").parquet(orders_path)
    order_products_prior.write.mode("overwrite").parquet(order_products_prior_path)
    products.write.mode("overwrite").parquet(products_path)
    aisles.write.mode("overwrite").parquet(aisles_path)
    departments.write.mode("overwrite").parquet(departments_path)

    # Passage des chemins via XCom
    return {
        "orders": orders_path,
        "order_products_prior": order_products_prior_path,
        "products": products_path,
        "aisles": aisles_path,
        "departments": departments_path
    }

def eda(**kwargs):
    spark = SparkSession.builder.getOrCreate()
    ti = kwargs['ti']
    paths = ti.xcom_pull(task_ids='load_and_clean_data')

    orders = spark.read.parquet(paths['orders'])
    order_products_prior = spark.read.parquet(paths['order_products_prior'])
    products = spark.read.parquet(paths['products'])
    aisles = spark.read.parquet(paths['aisles'])
    departments = spark.read.parquet(paths['departments'])

    # Analyse
    most_ordered, least_ordered = most_and_least_ordered_products(order_products_prior, products)
    most_ordered.show(10)
    least_ordered.show(10)

    order_ditribution = order_distribution_by_product(order_products_prior)
    order_ditribution.show(10)

    product_pairs_count = most_frequent_product_pairs(order_products_prior, orders, sample_size=10000, top_n=10)
    product_pairs_count.show(10)

    most_frequent, least_frequent = most_and_least_frequent_aisle_pairs(order_products_prior, products, aisles)
    most_frequent.show(10)

    houdly_distribution = hourly_order_distribution(orders)
    houdly_distribution.show(10)

    top_clients = get_top_users(orders)
    top_clients.show(10)

    dist_aisle, dist_dept = get_user_distribution(top_clients, order_products_prior, orders, products, aisles, departments)
    print("Distribution par rayons :")
    dist_aisle.show(truncate=False)

    print("Distribution par départements :")
    dist_dept.show(truncate=False)

def stop_spark(**kwargs):
    spark = SparkSession.builder.getOrCreate()
    spark.stop()

# ------------------ Définition des tâches ------------------
t1 = PythonOperator(task_id='init_spark', python_callable=init_spark, dag=dag)
t2 = PythonOperator(task_id='load_and_clean_data', python_callable=load_and_clean_data, dag=dag)
t3 = PythonOperator(task_id='eda', python_callable=eda, dag=dag)
t4 = PythonOperator(task_id='stop_spark', python_callable=stop_spark, dag=dag)
