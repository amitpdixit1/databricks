# Databricks notebook source
df_sales = spark.read.option("inferschema",True).option("Header",True).csv("dbfs:/FileStore/tables/Address.csv")
df_products = spark.read.csv("dbfs:/FileStore/tables/Customer.csv")
#display(df_products)
display(df_sales)

# COMMAND ----------

# DBTITLE 1,DF
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("designation",StringType(),True),
    StructField("department",StringType(),True),
    StructField("salary",DoubleType(),True)
])
data=[(1,"amit","barshi","azure DE","IT",200000.0),
      (1,"akshay","banglore","QA","Testing",220000.0),
      (2,"ajay","pune","developer","web",300000.5),
      (3,"john","mumbai","fullstackE","enterprise",40000.5),
      (4,"smita","kp","java","developer",250000.4),
      (5,"rekha","kharadi","outsytem","webfullstack",2030000.0),
      (2,"vineet","hydrabad","dot net","R&D",600000.6),
      (6,"prakash","toronto","AI","IT",5000.0),
      (7,"pranav","thane","ML","IT",70000.0),
      (8,"sachin","nashik","Angular Debeloper","IT",22000.0),
      (9,"rohit","delhi","azure DE","IT",560000.2),
      (10,"virat","gurgaon","azure DE","IT",234000.9),
      (12,"shubh","solapur","azure DE","IT",123000.0)]
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("employeee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employeee;
# MAGIC --select * from (select * ,DENSE_RANK() OVER(partition by department order by salary desc) as rn from employeee e)t
# MAGIC --where rn=2
# MAGIC
# MAGIC with cte as
# MAGIC (select * from (select * ,row_number() over(partition by id order by salary desc)as duplicate from employeee ))
# MAGIC delete from cte
# MAGIC where duplicate>1)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **INTERVIEW PREPERATION **CODES****

# COMMAND ----------

# Core Spark entry point
from pyspark.sql import SparkSession

# Schema definition types
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

# Functions (aggregates, window helpers, expressions, etc.)
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, expr,desc,  # column references & SQL expressions
    current_date, current_timestamp,  # date/time
    year, month, dayofmonth ,lead,lag,first,last           # date parts
)

# Window specification for advanced analytics
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Basic Aggregate
# df.printSchema()
# df.count()
#df.select("department").distinct().show()
#df.groupBy("department").avg("salary").show()
#df.groupBy("city").max("salary").show()
#df.groupBy("designation").min("salary").show()
#df.groupBy("department").sum("salary").show()
#df.groupBy("department").count().show()


# COMMAND ----------

# DBTITLE 1,Window Fucntions
###Rank Employees By Salary
# df.withColumn("rank", F.rank().over(Window.orderBy(F.desc("salary")))).show()
# windowSpec=Window.orderBy(col("id").desc())
# df.withColumn("rank",rank().over(windowSpec)).show()


### department wise rank
# dept_highest

#### department wise nth salary
# windowSpec = Window.partitionBy("department").orderBy(desc("salary"))
# df.withColumn("dept_highest", dense_rank().over(windowSpec)).filter(
#     col("dept_highest") == 5
# ).show()

###########Row number employees by city

# windowSpec=Window.partitionBy("city").orderBy(desc("salary"))
# df=df.withColumn("city_rn",row_number().over(windowSpec))
# display(df)

#### next salary in department LEAD function
# windowSpec=Window.partitionBy("department").orderBy(desc("salary"))
# df=df.withColumn("next_salary", lead("salary").over(windowSpec))
# display(df)

####cumulative sum
# windowSpec=Window.partitionBy("department").orderBy(desc("salary")).rowsBetween(Window.unboundedPreceding,1)
# df=df.withColumn("cum_sum",sum("salary").over(windowSpec)).show()

## find the duplicates
#df.groupBy("id").count().filter("count > 1").show()
#duplicate using window fucntion
# windowSpec=Window.partitionBy("salary").orderBy(desc("salary"))
# df=df.withColumn("count",row_number().over(windowSpec)).filter(col("count")>1).show()
# moving avg 
# windowSpec = Window.orderBy("id").rowsBetween(-1, 0)
# df.withColumn("moving_avg", F.avg("salary").over(windowSpec)).show()

#remove duplicates based on id keep highest salary

windowSpec = Window.partitionBy("id").orderBy(F.desc("salary"))
df.withColumn("rn", F.row_number().over(windowSpec)).filter("rn=1").drop("rn").show()

# COMMAND ----------

df.groupBy("id").count().show()


# COMMAND ----------

# DBTITLE 1,DF1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("EmployeeAnalytics").getOrCreate()
# Sample employee data
data = [
    (1, "Alice", "HR", 50000),
    (2, "Bob", "IT", 60000),
    (3, "Charlie", "Finance", 55000),
    (4, "David", "IT", 60000),
    (5, "Eve", "HR", 70000),
    (6, "Alice", "HR", 50000)  # duplicate
]
df1 = spark.createDataFrame(data, ["id", "name", "dept", "salary"])
display(df1)

# COMMAND ----------

df_joined=df.join(df1,df.id==df1.id,"left_anti").show()

# COMMAND ----------

from pyspark.sql.functions import max
df=df.select(max("salary"))
display(df)
df.explain()


# COMMAND ----------

# DBTITLE 1,CustomerDF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.appName("CustomerDF").getOrCreate()

# Sample data
names = ["Amit", "Riya", "Karan", "Sneha", "Vikram", "Priya", "Arjun", "Meera", "Rohit", "Neha",
         "Suresh", "Pooja", "Ankit", "Divya", "Manish", "Kavya", "Nikhil", "Shreya", "Raj", "Isha"]

data = []
base_time = datetime.now()

for i in range(20):
    data.append((
        i+1,  # id
        names[i % len(names)],  # name
        random.randint(100, 105),  # prod_id
        round(random.uniform(200, 2000), 2),  # cost
        (base_time - timedelta(days=random.randint(0, 10))).strftime("%Y-%m-%d %H:%M:%S")  # purchase_timestamp
    ))

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "prod_id", "cost", "purchase_timestamp"])
display(df)

# COMMAND ----------

# DBTITLE 1,Logging
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # INFO, DEBUG, WARNING, ERROR
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CustomerDFLogger")

# COMMAND ----------

# DBTITLE 1,Logg Metadata Of Customer DF
# Log schema
logger.info("DataFrame Schema:\n%s", df.printSchema())

# Log row count
row_count = df.count()
logger.info("Row count: %d", row_count)

# Log sample records
sample = df.limit(5).collect()
logger.info("Sample records: %s", sample)

# COMMAND ----------

# DBTITLE 1,Log Transformations
# Example transformation: max cost per product
agg_df = df.groupBy("prod_id").agg(max("cost").alias("max_cost"))

logger.info("Transformation applied: groupBy(prod_id) with max(cost)")
logger.info("Result schema:\n%s", agg_df.printSchema())
logger.info("Result sample:\n%s", agg_df.limit(5).collect())

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("CustomerDF").getOrCreate()

# ✅ Initialize log storage
log_records = []

# ✅ Helper function to add logs
def log_event(level, message):
    log_records.append({"level": level, "message": message})

# Example DataFrame (your df from earlier)
names = ["Amit", "Riya", "Karan", "Sneha", "Vikram", "Priya", "Arjun", "Meera", "Rohit", "Neha",
         "Suresh", "Pooja", "Ankit", "Divya", "Manish", "Kavya", "Nikhil", "Shreya", "Raj", "Isha"]

import random
from datetime import datetime, timedelta
base_time = datetime.now()
data = []
for i in range(20):
    data.append((
        i+1,
        names[i % len(names)],
        random.randint(100, 105),
        round(random.uniform(200, 2000), 2),
        (base_time - timedelta(days=random.randint(0, 10))).strftime("%Y-%m-%d %H:%M:%S")
    ))

df = spark.createDataFrame(data, ["id", "name", "prod_id", "cost", "purchase_timestamp"])

# ✅ Log DataFrame details
log_event("INFO", f"Schema: {df.schema.simpleString()}")
log_event("INFO", f"Row count: {df.count()}")
log_event("INFO", f"Sample: {df.limit(3).collect()}")

# ✅ Convert logs into Spark DataFrame for tabular display
log_df = spark.createDataFrame([Row(**rec) for rec in log_records])
display(log_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

windowSpec=Window.partitionBy("purchase_timestamp").orderBy(desc("id"))
df=df.withColumn("latest_entry",row_number().over(windowSpec)).filter(col("latest_entry")>1).limit(1)
display(df)

# COMMAND ----------

from pyspark.sql.functions import max

df.groupBy("name", "prod_id") \
  .agg(max("purchase_timestamp").alias("latest_purchase")) \
  .show()
