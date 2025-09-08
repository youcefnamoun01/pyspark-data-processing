
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F, types as T
import json
from pyspark.sql.types import StructType

load_dotenv()

bucket_name = os.getenv("BUCKET_NAME")

# Nettoyage chaînes de caractères
def clean_str(df, col_name):
    return df.withColumn(
        col_name,
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.lower(F.col(col_name)),
                            r'\\"', ""          # supprime les \"
                        ),
                        r'"+', ""              # supprime tous les "
                    ),
                    r'^\s+|\s+$', ""          # supprime espaces début/fin
                ),
                r'^-+|-+$', ""                # supprime les - au début et à la fin
            )
        )
    )


# Suppression des doublons
def remove_duplicates(df, subset):
    return df.dropDuplicates(subset)

# Gestion des valeurs nulles
def handle_nulls(df, numeric_fill=0, string_fill="null", bool_fill=False):
    for col_name, dtype in df.dtypes:
        if dtype in ["int", "bigint", "double", "float", "long", "decimal"]:
            df = df.fillna({col_name: numeric_fill})
        elif dtype in ["string"]:
            df = df.fillna({col_name: string_fill})
        elif dtype in ["boolean"]:
            df = df.fillna({col_name: bool_fill})
    return df

# Charger le schema depuis un fichier JSON
def load_schema(schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)

# Charger les données
def load_data(spark, file_name, inferSchema=True, sep=",", schema_folder="src/utils/schemas"):
    if inferSchema:
        # Lecture avec inférence de type
        df = spark.read.option("header", True).option("sep", sep) \
            .csv(f"s3a://{bucket_name}/{file_name}.csv", inferSchema=True)
    else:
        # Charger le schéma depuis JSON
        schema = load_schema(f"{schema_folder}/{file_name}_schema.json")
        df = spark.read.option("header", True).option("sep", sep) \
            .schema(schema) \
            .csv(f"s3a://{bucket_name}/{file_name}.csv")
    return df
