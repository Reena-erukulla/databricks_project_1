# Databricks notebook source
config = {
    "catalog": "main",   
    "source": {
        "catalog": "samples",
        "schema": "bakehouse"
    },
    "bronze": {
        "schema": "bronze",
        "tables": [
            "sales_transactions",
            "sales_customers",
            "sales_suppliers",
            "sales_franchises",
            "media_customer_reviews",
            "media_gold_reviews_chunked"
        ]
    },

    "silver": {
    "schema": "silver",
    "tables": [
        "df_transactions",
        "df_customers",
        "df_suppliers",
        "df_franchises",
        "df_customer_reviews",
        "df_gold_reviews",
        "df_final"
        ]
    }
}


# COMMAND ----------

