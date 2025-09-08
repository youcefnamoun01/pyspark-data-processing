import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, types as T
from utils.processing_functions import clean_str, remove_duplicates, handle_nulls,load_data
from pyspark.sql.functions import col, count, desc, asc, sum as sum_

load_dotenv()

# Environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("PySparkETL") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

#spark.sparkContext.setLogLevel("WARN")

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

"""
# Compter les commandes par heure
orders.groupBy("order_hour_of_day").count().orderBy("order_hour_of_day").show()

# Total commandes par utilisateur
top_users = orders.groupBy("user_id").count().orderBy(desc("count")).limit(10)

# Joindre avec order_products_prior et products pour rayon et department
user_orders = top_users.join(orders, "user_id") \
    .join(order_products_prior, "order_id") \
    .join(products, "product_id") \
    .join(aisles, "aisle_id") \
    .join(departments, "department_id")

# Distribution par rayons et départements
from pyspark.sql.functions import countDistinct

user_orders.groupBy("user_id", "aisle").count().orderBy("user_id", desc("count")).show()
user_orders.groupBy("user_id", "department").count().orderBy("user_id", desc("count")).show()



# Compter le nombre de fois qu'un produit a été commandé
product_counts = order_products_prior.groupBy("product_id") \
    .agg(count("*").alias("nb_commandes")) \
    .join(products, "product_id") \
    .select("product_id", "product_name", "nb_commandes")

# Produits les plus commandés
product_counts.orderBy(desc("nb_commandes")).show(10)

# Produits les moins commandés
product_counts.orderBy(asc("nb_commandes")).show(10)

# Distribution
distribution = product_counts.groupBy("nb_commandes").count().orderBy("nb_commandes")
distribution.show()

# Par exemple, prendre 10 000 commandes seulement
sample_orders = orders.filter(col("eval_set")=="prior").limit(10000)
sample_order_products = order_products_prior.join(sample_orders, "order_id")

# Puis refaire le self-join sur ce petit échantillon
op1 = sample_order_products.alias("op1")
op2 = sample_order_products.alias("op2")

product_pairs = op1.join(op2,(col("op1.order_id") == col("op2.order_id")) & (col("op1.product_id") < col("op2.product_id"))).select(
    col("op1.product_id").alias("prod1"),
    col("op2.product_id").alias("prod2")
)

product_pairs_count = product_pairs.groupBy("prod1", "prod2").count().orderBy(desc("count"))
product_pairs_count.show(10)

#Partie 2 : Paires de rayons les plus fréquentes

# Ajouter aisle_id aux produits
order_products_with_aisle = order_products_prior.join(products.select("product_id", "aisle_id"), "product_id")

# Aliases pour le self-join
op1 = order_products_with_aisle.alias("op1")
op2 = order_products_with_aisle.alias("op2")

# Paires d'aisles par commande
aisle_pairs = op1.join(op2,(col("op1.order_id") == col("op2.order_id")) & (col("op1.aisle_id") < col("op2.aisle_id"))).select(
    col("op1.aisle_id").alias("aisle1"),
    col("op2.aisle_id").alias("aisle2")
)

# Compter les fréquences
aisle_pairs_count = aisle_pairs.groupBy("aisle1", "aisle2").count().orderBy(desc("count"))

# Ajouter les noms des rayons
aisle_pairs_count = aisle_pairs_count \
    .join(aisles.select(col("aisle_id").alias("aisle1"), col("aisle").alias("aisle_name1")), "aisle1") \
    .join(aisles.select(col("aisle_id").alias("aisle2"), col("aisle").alias("aisle_name2")), "aisle2") \
    .select("aisle_name1", "aisle_name2", "count")

# Top 10
aisle_pairs_count.show(10, truncate=False)


"""



spark.stop()