from pyspark.sql.functions import count, desc, asc, desc, col

# Produits les plus et moins commandés
def most_and_least_ordered_products(order_products_prior, products, top_n=10):
    product_counts = (
        order_products_prior.groupBy("product_id")
        .agg(count("*").alias("nb_commandes"))
        .join(products, "product_id")
        .select("product_id", "product_name", "nb_commandes")
    )
    most_ordered = product_counts.orderBy(desc("nb_commandes")).limit(top_n)

    least_ordered = product_counts.orderBy(asc("nb_commandes")).limit(top_n)

    return most_ordered, least_ordered

# Distribution du nombre de commandes par produit
def order_distribution_by_product(order_products_prior):
    product_counts = (
        order_products_prior.groupBy("product_id")
        .agg(count("*").alias("nb_commandes"))
    )

    distribution = (
        product_counts.groupBy("nb_commandes")
        .count()
        .orderBy("nb_commandes")
    )

    return distribution

# Paires de produits les plus fréquemment commandées ensemble
def most_frequent_product_pairs(order_products, orders, sample_size=None, top_n=10):
    if sample_size is not None:
        orders = orders.limit(sample_size)
    
    order_products_joined = order_products.join(orders, "order_id")
    
    op1 = order_products_joined.alias("op1")
    op2 = order_products_joined.alias("op2")

    product_pairs = op1.join(
        op2,
        (col("op1.order_id") == col("op2.order_id")) & (col("op1.product_id") < col("op2.product_id"))
    ).select(
        col("op1.product_id").alias("prod1"),
        col("op2.product_id").alias("prod2")
    )

    product_pairs_count = product_pairs.groupBy("prod1", "prod2") \
        .count() \
        .orderBy(desc("count")) \
        .limit(top_n)

    return product_pairs_count

# Paires de rayons les plus fréquentes
def most_and_least_frequent_aisle_pairs(order_products, products, aisles, top_n=10):
    # Ajouter aisle_id aux produits
    order_products_with_aisle = order_products.join(products.select("product_id", "aisle_id"), "product_id")

    # Aliases pour le self-join
    op1 = order_products_with_aisle.alias("op1")
    op2 = order_products_with_aisle.alias("op2")

    # Paires d'aisles par commande
    aisle_pairs = op1.join(
        op2,
        (col("op1.order_id") == col("op2.order_id")) & (col("op1.aisle_id") < col("op2.aisle_id"))
    ).select(
        col("op1.aisle_id").alias("aisle1"),
        col("op2.aisle_id").alias("aisle2")
    )

    # Compter les fréquences
    aisle_pairs_count = aisle_pairs.groupBy("aisle1", "aisle2").count()

    # Ajouter les noms des rayons
    aisle_pairs_count = aisle_pairs_count \
        .join(aisles.select(col("aisle_id").alias("aisle1"), col("aisle").alias("aisle_name1")), "aisle1") \
        .join(aisles.select(col("aisle_id").alias("aisle2"), col("aisle").alias("aisle_name2")), "aisle2") \
        .select("aisle_name1", "aisle_name2", "count")

    most_frequent = aisle_pairs_count.orderBy(desc("count")).limit(top_n)
    least_frequent = aisle_pairs_count.orderBy(asc("count")).limit(top_n)

    return most_frequent, least_frequent

# Distribution horaire des commandes
def hourly_order_distribution(orders):
    distribution = orders.groupBy("order_hour_of_day") \
                         .count() \
                         .orderBy("order_hour_of_day")
    return distribution


def get_top_users(orders, top_n=10):
    top_clients = orders.groupBy("user_id") \
                        .count() \
                        .orderBy(desc("count")) \
                        .limit(top_n) \
                        .select("user_id")
    return top_clients


def get_user_distribution(top_clients, order_products, orders, products, aisles, departments):
    # Joindre pour récupérer toutes les informations
    user_orders = top_clients.join(orders, "user_id") \
                             .join(order_products, "order_id") \
                             .join(products, "product_id") \
                             .join(aisles, "aisle_id") \
                             .join(departments, "department_id")
    
    # Distribution par rayons
    distribution_aisle = user_orders.groupBy("user_id", "aisle") \
                                    .count() \
                                    .orderBy("user_id", desc("count"))
    
    # Distribution par départements
    distribution_department = user_orders.groupBy("user_id", "department") \
                                         .count() \
                                         .orderBy("user_id", desc("count"))
    
    return distribution_aisle, distribution_department