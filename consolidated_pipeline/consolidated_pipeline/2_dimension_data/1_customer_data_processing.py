# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/muktalondhepatil@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catlog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catlog")
data_source = dbutils.widgets.get("data_source")
print(catalog,data_source)

# COMMAND ----------

catalog = dbutils.widgets.get("catlog")
data_source = dbutils.widgets.get("data_source")
base_path = f's3://sportsprod-db/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = spark.read.format("csv").load(base_path)
display(df.limit(10))



# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Processing**

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------


print(f"data_source parameter: {data_source}")
print(f"Reading table: {catalog}.{bronze_schema}.{data_source}")
df_bronze.printSchema()

# COMMAND ----------


df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name", F.trim(F.col("customer_name"))
)


# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

from pyspark.sql import functions as F

# Step 1: Mapping for incorrect spellings
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',
    
    'Hyderbad': 'Hyderabad',
    'Hyderabadd': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

# Step 2: Allowed valid cities
allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

# Step 3: Clean + standardize city column
df_silver = (
    df_silver
    # normalize (optional but recommended for real data)
    .withColumn("city", F.trim(F.col("city")))
    
    # replace incorrect values
    .replace(city_mapping, subset=["city"])
    
    # keep only valid cities, others → NULL
    .withColumn(
        "city",
        F.when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

# Step 4: Check distinct values
df_silver.select("city").distinct().show()


    

# COMMAND ----------

  df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# Title case fix
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap(F.col("customer_name")))
)
df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

customer_city_fix = {
    789403: 'New Delhi',
    789420: 'Bengaluru',
    789521:'Hyderabad',
    789603: 'Hyderabad',
}
df_fix = spark.createDataFrame(
[(k,v) for k,v in customer_city_fix.items()],
["customer_id","fixed_city"]
)
display(df_fix)



# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix,'customer_id','left')
    .withColumn(
    "city",
    F.coalesce("city","fixed_city")
    )
    .drop("fixed_city")
)
display(df_silver)


# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_id",
    F.col("customer_id").cast("int")
)
print(df_silver.printSchema())

# COMMAND ----------

df_silver =(
    df_silver
    .withColumn(
        "customer",
        F.concat_ws(F.lit("-"), "customer_name", F.coalesce("city", F.lit("unknown")))
    )
    .withColumn(
        "market",
        F.lit("India"))
    .withColumn("platform",F.lit("Sports Bar"))
    .withColumn("channel",F.lit("Acquisition"))
)
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
 .format("delta")\
 .option("delta.enableChangeDataFeed", "true")\
 .mode("overwrite")\
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #**Gold **Processin**g**

# COMMAND ----------

df_silver = spark.sql(f"select * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")


# COMMAND ----------

df_gold.write\
 .format("delta")\
 .option("delta.enableChangeDataFeed", "true")\
 .mode("overwrite")\
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_customers")
df_child_customer = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)


# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customer.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()