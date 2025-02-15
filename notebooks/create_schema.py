# %%

import sys
print(sys.executable)

# %%
import pyspark
print(pyspark.__file__)

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Test") \
    .getOrCreate()

print(spark)


# %%
pyspark.__version__

# %%
rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Edgar"), (6, "Frank"), (7, "Grace")])

# %%
from pyspark.sql import Row

# create dataframe
df = rdd.map(lambda x: Row(id=x[0], first_name=x[1]))


# %%
df1 = spark.createDataFrame(df)

# %%
# Show the dataframe
df1.show()

# %%
df1.printSchema()

# %%
# It works with DataFrame but when it comes to 
# StructType, it didn't work

# %%
# Correct way to define schema
from pyspark.sql import types
# Define schema
schema1 = types.StructType([
    types.StructField("id", types.IntegerType(), True),
    types.StructField("first_name", types.StringType(), True)
])

# %%
# update2 DataFrame with schema
df2 = spark.createDataFrame(df, schema1)

# %%
df2.printSchema()


