import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, types as T
from utils.processing_functions import clean_str, remove_duplicates, handle_nulls,load_data
from utils.eda_functions import most_and_least_ordered_products, order_distribution_by_product, most_frequent_product_pairs, most_and_least_frequent_aisle_pairs, hourly_order_distribution, get_top_users, get_user_distribution

load_dotenv()

# Variables d'environnement
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("PySparkETL") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- Traitement des données -------------

# Chargement des données
orders = load_data(spark, "orders")
order_products_prior = load_data(spark, "order_products__prior")
products = load_data(spark, "products")
aisles = load_data(spark , "aisles")
departments = load_data(spark, "departments")

# Nettoyage des chaînes de caractères
products = clean_str(products, "product_name")
aisles = clean_str(aisles, "aisle")
departments = clean_str(departments, "department")

# Suppression des doublons
order_products_prior = remove_duplicates(order_products_prior, ["order_id", "product_id"])
products = remove_duplicates(products, ["product_id"])
orders = remove_duplicates(orders, ["order_id"])
aisles = remove_duplicates(aisles,["aisle_id"])
departments = remove_duplicates(departments, ["department_id"])

# Traitement des valeurs nulles
order_products_prior = handle_nulls(order_products_prior)
products = handle_nulls(products)
orders = handle_nulls(orders)
aisles = handle_nulls(aisles)
departments = handle_nulls(departments)

# ------------ Analyse exploratoire des données --------------

# Produits les plus et moins commandés
most_ordered, least_ordered = most_and_least_ordered_products(order_products_prior, products)
most_ordered.show(10)
least_ordered.show(10)

# Distribution du nombre de commandes par produit
order_ditribution = order_distribution_by_product(order_products_prior)
order_ditribution.show(10)

# Paires de produits les plus fréquemment commandées ensemble
product_pairs_count = most_frequent_product_pairs(order_products_prior, orders, sample_size=10000, top_n=10)
product_pairs_count.show(10)

""""
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

"""
spark.stop()