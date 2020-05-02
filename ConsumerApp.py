import sys
from datetime import date
import datetime
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

class Main:
    spark = SparkSession.builder \
        .master("local") \
        .appName("emarket") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test-4") \
        .load()

    casted = df.selectExpr("CAST(value AS STRING)")

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
        StructField("customer", StructType(customerSchema), True),
        StructField("product", StructType(productSchema), True),
        StructField("timeValue", StringType(), True),
        StructField("productCount", IntegerType(), True)

    ])

    eMarketRawData = casted.select(from_json("value", shoppingBasketSchema).alias("jsontostructs")).select(
        "jsontostructs.*")

    # QUERIES

    # TOP 10 CITY
    top10city = eMarketRawData.groupBy("customer.cityCode").count()

    # CUSTOMER-PRODUCT WITH COUNT
    eMarketRawData.groupBy("customer.customerId", "product.productName").count().sort(asc("customerId"))

    # TOTAL COSTS
    costData = eMarketRawData.withColumn("totalCost",
                                         eMarketRawData['product.productCost'] * eMarketRawData['productCount'])
    totalCostData = costData.groupBy("customer.customerId").sum("totalCost").sort(desc("sum(totalCost)"))


    # TOP PRODUCT with productCount
    productCounts = eMarketRawData.groupBy("product.productId", "product.productName").sum("productCount").sort(
        desc("sum(productCount)"))

    # LEAST SOLD PRODUCTS with different sorts
    eMarketRawData.groupBy("product.productId", "product.productName").sum("productCount").sort(
        asc("sum(productCount)"))
    eMarketRawData.groupBy("product.productId", "product.productName").sum("productCount").sort(
        asc("product.productId"))

    # CITY'S TOP PRODUCTS
    cityProductCounts = eMarketRawData.groupBy("customer.cityCode", "product.productName").sum("productCount").sort(
        desc("cityCode"))

    cityProductCounts.groupBy("cityCode").agg(collect_list("productName")).sort(asc("cityCode"))

    # cityProductCounts.groupBy("cityCode").agg(functions.first("productName").alias("productName"), functions.max("sum(productCount)"))

    # cityProductCounts.groupBy("cityCode").agg(functions.first("productName").alias("productName"), functions.min("sum(productCount)"))

    #  AGE OF CUSTOMERs
    withAgeData = eMarketRawData.withColumn("Age", (
            datediff(current_date(), to_date(eMarketRawData["customer.dateOfBirth"], "yyyy-MM-dd")) / 365).cast(IntegerType()))

    #AVG OF CUSTOMERs AGEs
    withAgeData.agg(functions.avg("Age"))

    #AVG AGE OF CUSTOMER per CITY
    withAgeData.groupBy("customer.cityCode").agg(functions.avg("Age")).sort(asc("cityCode"))


