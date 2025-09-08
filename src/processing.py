
from pyspark.sql import SparkSession, functions as F, types as T


# Nettoyage chaînes de caractères
def clean_str(col):
    return (
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.lower(F.col(col)),
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


# Charger les données
def load_data(spark,path, header=True, inferSchema=True):
    return spark.read.csv(path, header=header, inferSchema=inferSchema)



