# Databricks notebook source
# MAGIC %run ./config_notebook
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils_notebook

# COMMAND ----------

df_transactions = spark.table(f"{config['bronze']['schema']}.sales_transactions")

# COMMAND ----------

df_transactions.show()

# COMMAND ----------

df_suppliers=spark.table(f"{config['bronze']['schema']}.sales_suppliers")
df_suppliers.show()

# COMMAND ----------

df_franchises=spark.table(f"{config['bronze']['schema']}.sales_franchises")
df_franchises.show()

# COMMAND ----------

df_customers=spark.table(f"{config['bronze']['schema']}.sales_customers")
df_customers.show()

# COMMAND ----------

df_customer_reviews=spark.table(f"{config['bronze']['schema']}.media_customer_reviews")
df_customer_reviews.show()

# COMMAND ----------

df_gold_reviews=spark.table(f"{config['bronze']['schema']}.media_gold_reviews_chunked")
df_gold_reviews.show()

# COMMAND ----------

df_transactions=remove_duplicates(df_transactions)


# COMMAND ----------

df_suppliers=remove_duplicates(df_suppliers)
df_franchises=remove_duplicates(df_franchises)
df_customers=remove_duplicates(df_customers)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import count,col
window=Window.partitionBy("transactionID")
df_transactions=df_transactions.withColumn("count",count("transactionID").over(window))
df_transactions.filter(col("count")>1).show()

# COMMAND ----------

df_transactions=drop_nulls(df_transactions,["transactionID"])
df_customer_reviews=drop_nulls(df_customer_reviews,["new_id"])
df_gold_reviews=drop_nulls(df_gold_reviews,["chunk_id"])
df_suppliers=drop_nulls(df_suppliers,["supplierID"])
df_franchises=drop_nulls(df_franchises,["franchiseID"])
df_customers=drop_nulls(df_customers,["customerID"])

# COMMAND ----------

df_transactions.filter(col("transactionID").isNull()).show()

# COMMAND ----------

df_transactions.printSchema()

# COMMAND ----------

df_transactions =df_transactions.withColumn("unitprice",col("unitprice").cast("double"))\
    .withColumn("totalprice",col("totalprice").cast("double"))

# COMMAND ----------

df_transactions =df_transactions.drop("count")

# COMMAND ----------

df_suppliers.printSchema()

# COMMAND ----------

df_suppliers=df_suppliers.withColumn("approved",col("approved").cast("boolean"))

# COMMAND ----------

df_franchises.printSchema()

# COMMAND ----------

df_customers.printSchema()


# COMMAND ----------

df_customers=df_customers.withColumn("postal_zip_code",col("postal_zip_code").cast("string"))


# COMMAND ----------

df_customer_reviews.printSchema()

# COMMAND ----------

df_customer_reviews=df_customer_reviews.withColumn("new_id",col("new_id").cast("long"))

# COMMAND ----------

df_gold_reviews.printSchema()


# COMMAND ----------

df_gold_reviews=df_gold_reviews.withColumn("franchiseID",col("franchiseID").cast("long"))

# COMMAND ----------

df_customers = df_customers.drop("ingestion_date")
df_franchises = df_franchises.drop("ingestion_date")
df_suppliers = df_suppliers.drop("ingestion_date")
df_customer_reviews = df_customer_reviews.drop("ingestion_date")
df_gold_reviews = df_gold_reviews.drop("ingestion_date")

# COMMAND ----------

df_customer_reviews = rename_column(
    df_customer_reviews, "review_date", "customer_review_date"
)

df_gold_reviews = rename_column(
    df_gold_reviews, "review_date", "gold_review_date"
)

# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"name","supplier_name")







# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"size","supplier_size")

# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"city","supplier_city")

# COMMAND ----------

df_franchises=rename_column(df_franchises,"name","franchise_name")

# COMMAND ----------

df_franchises=rename_column(df_franchises,"size","franchise_size")

# COMMAND ----------

df_franchises=rename_column(df_franchises,"city","franchise_city")

# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"latitude","supplier_latitude")
df_suppliers=rename_column(df_suppliers,"longitude","supplier_longitude")
df_franchises=rename_column(df_franchises,"latitude","franchise_latitude")
df_franchises=rename_column(df_franchises,"longitude","franchise_longitude")


# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"country","supplier_country")

# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"district","supplier_district")

# COMMAND ----------

df_suppliers=rename_column(df_suppliers,"continent","supplier_continent")


# COMMAND ----------

df_franchises=rename_column(df_franchises,"country","franchise_country")



# COMMAND ----------

df_franchises=rename_column(df_franchises,"district","franchise_district")

# COMMAND ----------

df_franchises=rename_column(df_franchises,"continent","franchise_continent")

# COMMAND ----------

df_franchise_1 = df_franchises \
    .join(df_suppliers, "supplierID", "left") \
    .join(df_customer_reviews, "franchiseID", "left") \
    .join(df_gold_reviews, "franchiseID", "left")

# COMMAND ----------

df_final = df_transactions \
    .join(df_customers, "customerID", "left") \
    .join(df_franchise_1, "franchiseID", "left")

# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

df_final.groupBy("transactionID").count().filter(col("count") > 1).show()

# COMMAND ----------

from pyspark.sql.functions import collect_list
df_final = df_final.withColumn(
    "reviews",
    collect_list("review").over(Window.partitionBy("transactionID"))
)



# COMMAND ----------

df_final = df_final.dropDuplicates(["transactionID"])

# COMMAND ----------

df_final.show()

# COMMAND ----------

df_final = df_final.drop("cardNumber")

# COMMAND ----------

df_final = df_final.drop("new_id", "chunk_id", "review_uri")

# COMMAND ----------

df_final=rename_column(df_final,"unitprice", "unit_price")
df_final=rename_column(df_final,"totalprice", "total_price")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_final = df_final.withColumn("review", regexp_replace("review", "\n", " ")) \
       .withColumn("chunked_text", regexp_replace("chunked_text", "\n", " "))

# COMMAND ----------

df_final=auto_fill_nulls(df_final)

# COMMAND ----------

df_final.show()

# COMMAND ----------

df_final.filter(col("total_price") != col("quantity") * col("unit_price")).count()

# COMMAND ----------

df_final.filter(col("supplier_name") == "Unknown").count()

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, weekofyear 
df_final = df_final.withColumn("year", year(col("dateTime"))) \
       .withColumn("month", month(col("dateTime"))) \
       .withColumn("day", dayofmonth(col("dateTime"))) \
       .withColumn("week", weekofyear(col("dateTime")))

# COMMAND ----------

df_transactions.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.sales_transactions")

# COMMAND ----------

df_suppliers.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_suppliers")
df_franchises.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_franchises")


# COMMAND ----------

df_customer_reviews.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_customer_reviews")
df_customers.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_customers")
df_gold_reviews.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_gold_reviews")

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.df_final")

# COMMAND ----------

display(spark.table("silver.df_final"))

# COMMAND ----------

from pyspark.sql.functions import col, sum
 
df_final.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_final.columns
]).show()
 

# COMMAND ----------

