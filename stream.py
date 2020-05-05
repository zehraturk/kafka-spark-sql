import sys
import datetime
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@udf(returnType=LongType())
def multiply(column1, column2):
    return column1 * column2


class Main:
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("emarket-stream-v3") \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test-6") \
        .load()

    castedData = df.selectExpr("CAST(value AS STRING)")

    customerSchema = StructType([
        StructField("customerId", IntegerType(), True),
        StructField("cityCode", IntegerType(), True),
        StructField("phoneNumber", StringType(), True),
        StructField("dateOfBirth", StringType(), True)

    ])

    productSchema = StructType([
        StructField("productId", IntegerType(), True),
        StructField("productName", StringType(), True),
        StructField("productCost", FloatType(), True)

    ])

    shoppingBasketSchema = StructType([
        StructField("customer", customerSchema, True),
        StructField("product", productSchema, True),
        StructField("timeValue", StringType(), True),
        StructField("productCount", IntegerType(), True)

    ])
    eMarketRawData = castedData.select(from_json("value", shoppingBasketSchema).alias("jsontostructs")).select(
        "jsontostructs.*")

    # CAST string timeValue column to timestamp
    timeValueData = eMarketRawData.withColumn("current_ts",
                                              to_timestamp(eMarketRawData["timeValue"], '["yyyy-MM-dd HH:mm:ss"]'))

    # COUNT of purchases  per minute
    countPerMinute = timeValueData.groupBy(functions.window("current_ts", "1 minute"), ).count()

    timeValueDataWithCosts = timeValueData.withColumn("totalCost", timeValueData["productCount"] * timeValueData[
        "product.productCost"])

    # TOTAL COST per window
    windowsTotalCosts = timeValueDataWithCosts.groupBy(window("current_ts", "1 minute")).sum("totalCost")

    # TOTAL COST per window with cityCode order by desc cost
    windowsTotalCostsWithCity = timeValueDataWithCosts.groupBy(window("current_ts", "1 minute"),
                                                               "customer.cityCode").sum("totalCost").sort(
        functions.desc("sum(totalCost)"))

    # TOTAL COST per window with customerId order by desc cost
    windowsTotalCostsWithCustomerId = timeValueDataWithCosts.groupBy(window("current_ts", "1 minute"),
                                                                     "customer.customerId") \
        .sum("totalCost").sort(functions.desc("sum(totalCost)"))

    streamingQuery = windowsTotalCostsWithCustomerId.writeStream.format("console").outputMode("complete").option(
        "truncate", "false").start()
    streamingQuery.awaitTermination()
