from pyspark.sql import SparkSession, functions as F, types as T
from processing import clean_str, remove_duplicates, handle_nulls
from pyspark.sql.functions import col, count, desc, asc, sum as sum_


# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TP PySpark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


# Chargement des données
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
order_products_prior = spark.read.csv("data/order_products__prior.csv", header=True, inferSchema=True)
products = spark.read.csv("data/products.csv", header=True, inferSchema=True)
aisles = spark.read.csv("data/aisles.csv", header=True, inferSchema=True)
departments = spark.read.csv("data/departments.csv", header=True, inferSchema=True)

# Nettoyage des chaînes de caractères
products = products.withColumn("product_name", clean_str("product_name"))
aisles = aisles.withColumn("aisle", clean_str("aisle"))
departments = departments.withColumn("department", clean_str("department"))


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


"""
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

"""""""""""""""""""""
#Partie 2 : Paires de rayons les plus fréquentes
"""""""""""
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