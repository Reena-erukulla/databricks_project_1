# Databricks notebook source
def remove_duplicates(df,col=None):
    return df.dropDuplicates(col)

def drop_nulls(df,cols):
    return df.dropna(subset=cols)

def fill_nulls(df,col_values):
    return df.fillna(col_values)

def rename_column(df,old,new):
    return df.withColumnRenamed(old,new)
from pyspark.sql.types import *
from pyspark.sql.functions import lit

def auto_fill_nulls(df):
    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType

        if isinstance(dtype, StringType):
            df = df.fillna({col_name: "Unknown"})

        elif isinstance(dtype, (IntegerType, LongType)):
            df = df.fillna({col_name: 0})

        elif isinstance(dtype, DoubleType):
            df = df.fillna({col_name: 0.0})

        elif isinstance(dtype, BooleanType):
            df = df.fillna({col_name: False})

        elif isinstance(dtype, TimestampType):
            df = df.fillna({col_name: "1900-01-01 00:00:00"})

    return df


# COMMAND ----------

