# Databricks notebook source
spark

# COMMAND ----------

storage_account = 'olistadlsstorage'
application_id = '5002e051-dd14-43d5-8bb6-6ec9178e3421'
directory_id = '396742ca-758c-4834-ad67-1276e5f9e126'

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", '<secret>')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

container = "olist"
storage_account = "olistadlsstorage"
customer_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/olist_customers_dataset.csv")


# COMMAND ----------

display(customer_df)

# COMMAND ----------

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

# COMMAND ----------

orders_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(orders_path)
payemnts_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(payments_path)
reviews_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(reviews_path)
items_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(items_path)
customers_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(customers_path)
sellers_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(geolocation_path)
products_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(products_path)

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "rfuce.h.filess.io"
database = "olistmongo_goldvoice"
port = "27018"
username = "olistmongo_goldvoice"
password = "2a83152114ae0a0d61308d2fd71f35b6043d15bd"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]


# COMMAND ----------

import pandas as pd
collection = mydatabase["product_categories"]

mongo_data = pd.DataFrame(list(collection.find()))

mongo_data.head()

# COMMAND ----------

display(products_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def clean_data(df,name):
    print("cleaning " + name)
    return df.dropDuplicates().na.drop('all')

orders_df = clean_data(orders_df,"orders_df")
display(orders_df)

# COMMAND ----------

# converting timestamp column to date columns
orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col('order_purchase_timestamp')))\
    .withColumn("order_delivered_customer_date", to_date(col('order_delivered_customer_date')))\
    .withColumn("order_estimated_delivery_date", to_date(col('order_estimated_delivery_date')))

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# calculating delivery and time delays

orders_df = orders_df.withColumn("actual_delivery_time", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))\
    .withColumn("estimated_delivery_time", datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")))

orders_df = orders_df.withColumn("delay", col("actual_delivery_time") - col("estimated_delivery_time"))

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining

# COMMAND ----------

orders_customer_df = orders_df.join(customers_df,orders_df.customer_id == customers_df.customer_id,'left')

order_payments_df = orders_customer_df.join(payemnts_df,orders_customer_df.order_id == payemnts_df.order_id,'left')

order_items_df = order_payments_df.join(items_df, 'order_id','left')

order_items_products_df = order_items_df.join(products_df, order_items_df.product_id == products_df.product_id,'left')

final_df = order_items_products_df.join(sellers_df, order_items_products_df.seller_id == sellers_df.seller_id,'left')

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We need to join final df with mongo db data 
# MAGIC Mongo data is in pandas Dataframe we need to change to spark dataframe to perform join operation

# COMMAND ----------

mongo_data.head()

# COMMAND ----------

mongo_data.drop('_id', axis=1, inplace=True)

# COMMAND ----------

mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df)

# COMMAND ----------

final_df  = final_df.join(mongo_spark_df,'product_category_name','left')

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We nedd to ssave the final df before that there are some duplicate columns in final df so we are droping those columns

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)    
        
    return df.drop(*columns_to_drop)
final_df = remove_duplicate_columns(final_df)
display(final_df)


# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").save("abfss://olist@olistadlsstorage.dfs.core.windows.net/silver/")

# COMMAND ----------

