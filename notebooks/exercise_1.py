# %%
import pyspark

from pyspark.sql import SparkSession

# %%
spark = SparkSession.builder \
    .appName("Exercise number 1") \
    .master("local[*]") \
    .getOrCreate()

print(spark)

# %%
rdd = spark.sparkContext.parallelize([
    (1,'2013-07-25 00:00:00',11599, 'CLOSED'),
    (2,'2013-07-25 00:00:00',256, 'PENDING_PAYMENT'),
    (3,'2013-07-25 00:00:00',12111, 'COMPLETE'),
    (4,'2013-07-25 00:00:00',8827, 'CLOSED'),
    (5,'2013-07-25 00:00:00',11318, 'COMPLETE'),
    (6,'2013-07-25 00:00:00',7130, 'COMPLETE'),
    (7,'2013-07-25 00:00:00',4530, 'COMPLETE'),
    (8,'2013-07-25 00:00:00',2911, 'PROCESSING'),
    (9,'2013-07-25 00:00:00',5657, 'PENDING_PAYMENT'),
    (10,'2013-07-25 00:00:00',5648, 'PENDING_PAYMENT'),
    (11,'2013-07-25 00:00:00',918, 'PAYMENT_REVIEW'),
    (12,'2013-07-25 00:00:00',1837, 'CLOSED'),
    (13,'2013-07-25 00:00:00',9149, 'PENDING_PAYMENT'),
    (14,'2013-07-25 00:00:00',9842, 'PROCESSING'),
    (15,'2013-07-25 00:00:00',2568, 'COMPLETE'),
    (16,'2013-07-25 00:00:00',7276, 'PENDING_PAYMENT'),
    (17,'2013-07-25 00:00:00',2667, 'COMPLETE'),
    (18,'2013-07-25 00:00:00',1205, 'CLOSED'),
    (19,'2013-07-25 00:00:00',9488, 'PENDING_PAYMENT'),
    (20,'2013-07-25 00:00:00',9198, 'PROCESSING')
])

# %%
from pyspark.sql import Row
# Create a DataFrame
df = rdd.map(lambda x: Row(order_id=x[0], order_date=x[1], customer_id=x[2], status= x[3]))

# %%
# # Update date typ of order_date
# from pyspark.sql import functions as F
# df = df.map(lambda x: F.to_date(x.order_date, 'yyyy-MM-dd HH:mm:ss'))

# %%
# create a schema
from pyspark.sql import types

schema = types.StructType([
    types.StructField('order_id', types.IntegerType(), True),
    types.StructField('order_date', types.StringType(), True),
    types.StructField('customer_id', types.IntegerType(), True),
    types.StructField('status', types.StringType(), True)
])

# %%
# Create DataFrame
df1 = spark.createDataFrame(df,schema)

# %%
df1.printSchema()

# %%
df1.show()

# %%
from pyspark.sql import functions as F

df2 = df1.withColumn('order_date',F.to_date(df1.order_date, 'yyyy-MM-dd HH:mm:ss'))

# %%
# Check the update of order_date
df2.printSchema()        


# %%
df2.show(5)

# %%
from pyspark.sql.functions import *
# add new column
transformed_df2 = df2.withColumn('new_id_cust',(col("customer_id") + 1000).cast("string"))

# %%
transformed_df2.printSchema()

# %%
# Rename the existing column
transformed_df2_2 = transformed_df2.withColumnRenamed('order_status','current_status')

# %%
transformed_df2_2.show(5)

# %%
# final file: first solution
final_df = transformed_df2_2.select('order_id','status')
final_df.show(5)

# %%
# final file: second solution
final_df1= transformed_df2_2.drop('order_date','customer_id','new_id_cust')
final_df1.show(5)

# %%
# write .csv file
final_df1.write.csv('D:/repos/final_csv_test.csv')


