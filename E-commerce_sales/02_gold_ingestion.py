# Databricks notebook source
# MAGIC %run ./config_notebook
# MAGIC

# COMMAND ----------

df_gold=spark.table(f"{config["silver"]["schema"]}.df_final")

# COMMAND ----------

df_gold.select("dateTime").distinct().show(40, False)

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

df_gold = df_gold.select(
    "transactionID",
    "customerID",
    "franchiseID",
    "product",
    "quantity",
    "unit_price",
    "total_price",
    "paymentMethod",
    "year",
    "month",
    "week",
    "city",
    "state",
    "country",
    "franchise_name",
    "franchise_city",
    "franchise_country",
    "supplier_name",
    "ingredient",
    "reviews"
)

# COMMAND ----------

from pyspark.sql.functions import *

df_total_revenue = df_gold.agg(
    round(sum("total_price"), 2).alias("total_revenue")
)

df_franchise_revenue = df_gold.groupBy("franchise_name") \
    .agg(round(sum("total_price"), 2).alias("franchise_revenue")) \
    .orderBy(col("franchise_revenue").desc())

df_monthly_revenue = df_gold.groupBy("year", "month") \
    .agg(round(sum("total_price"), 2).alias("monthly_revenue"))

df_customer_spending = df_gold.groupBy("customerID") \
    .agg(round(sum("total_price"), 2).alias("total_spent"))

df_payment_method = df_gold.groupBy("paymentMethod") \
    .agg(count("transactionID").alias("usage_count"))

df_average_order_value = df_gold.agg(
    round(sum("total_price") / count("transactionID"), 2).alias("avg_order_value")
)

# COMMAND ----------

df_total_revenue.show()
df_franchise_revenue.show()
df_monthly_revenue.show()
df_customer_spending.show()
df_payment_method.show()
df_average_order_value.show()


# COMMAND ----------

df_customer_spending = df_customer_spending.orderBy(col("total_spent").desc())
df_payment_method = df_payment_method.orderBy(col("usage_count").desc())

# COMMAND ----------

df_total_revenue.write.format("delta").mode("overwrite").option("overwriteSchema","true") .saveAsTable("gold.total_revenue")

df_franchise_revenue.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.franchise_revenue")

df_monthly_revenue.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.monthly_revenue")

df_customer_spending.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.customer_spending")

df_payment_method.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.payment_method")

df_average_order_value.write.format("delta").mode("overwrite").saveAsTable("gold.avg_order_value")

# COMMAND ----------

display(spark.table("gold.avg_order_value"))
display(spark.table("gold.customer_spending"))
display(spark.table("gold.franchise_revenue"))
display(spark.table("gold.monthly_revenue"))
display(spark.table("gold.payment_method"))
display(spark.table("gold.total_revenue"))

# COMMAND ----------

df_product_orders = df_gold.groupBy("product") \
    .agg(count("transactionID").alias("total_orders"))
df_product_performance = df_gold.groupBy("product") \
    .agg(
        sum("quantity").alias("total_quantity"),
        round(sum("total_price"), 2).alias("total_revenue")
    )

# COMMAND ----------

df_product_orders.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.product_orders")
df_product_performance.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.product_performance")
display(spark.table("gold.product_orders"))
display(spark.table("gold.product_performance"))

# COMMAND ----------

df_total_orders = df_gold.agg(count("transactionID").alias("total_orders"))

# COMMAND ----------

df_total_orders.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold.total_orders")
display(spark.table("gold.total_orders"))


# COMMAND ----------



# COMMAND ----------

