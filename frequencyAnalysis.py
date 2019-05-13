# -*- coding: utf-8 -*-
"""
Created on Sat Apr 6 19:36:08 2019

@author: reza
"""

###############################################################################
#
# IMPORT MODULES
#
###############################################################################

#import sys
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, udf, lit
from math import sin, cos, radians, atan2, sqrt

###############################################################################
#
# CREATE SPARKSESSION
#
###############################################################################

spark = SparkSession \
    .builder \
    .appName("LocalSparkSession") \
    .master("local[2]") \
    .getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

###############################################################################
#
# Load CSVs without providing schema
#
###############################################################################

# Avoid inferschema as it requires reading all data
# Raw data for february 1st
filePath = "a51cca8e-48b8-4301-bb63-3f1ebfb3dd00.csv"
#filePath = "jln_maroof_bangsar.csv"
mainDf = spark.read.option("header", "True").csv(filePath)

# Show schema
mainDf.printSchema()
mainDf.show()

#mainDf.select("idfa").count()   # Total IDFAs: 162,015,992
#mainDf.select("idfa").distinct().count() # Unique IDFAs: 699,020

###############################################################################
#
# Prepare dataframe for analysis
#
###############################################################################

# Select features of dataset for dwelltime and frequency analysis
sampleDf = mainDf.selectExpr("idfa as advertisement_id",
                                "round(cast(latitude as float), 4) latitude",
                                "round(cast(longitude as float), 4) longitude",
                                "round(cast(horizontalaccuracy as float), 2) horizontal_accuracy",
                                "cast(cast(timestamp as int) as timestamp) date_time")
#locationDf.show()
sampleDf.printSchema()
sampleDf.show()

###############################################################################
#
# Dwell Time Analysis
#
###############################################################################

# Jalan Maroof Bangsar
#billboardCoordinates = (3.1423, 101.6642)
billboardCoordinates = (3.1439, 101.7068)

#billboardCoordinates = [[3.1423, 101.6642]]
#schema = StructType([StructField("lat", FloatType(), True),
#                     StructField("lon", FloatType(), True)])
#
## Create dataframe from billboard coordinates
#billboardLocation = spark.createDataFrame(billboardCoordinates, schema)

# Create Haversine formula
def getDistance(billboardLat, billboardLon, adIdLat, adIdLon):
    # Transform to radians
    billboardLat, billboardLon, adIdLat, adIdLon = \
        map(radians, [billboardLat, billboardLon, adIdLat, adIdLon])
    delLatitude = adIdLat - billboardLat
    delLongitude = adIdLon - billboardLon
    
    # Calculate area
    area = (sin(delLatitude / 2)) ** 2 + cos(billboardLat) * cos(adIdLat) * \
            (sin(delLongitude / 2)) ** 2
    
    # Calculate the central angle
    central_angle = 2 * atan2(sqrt(area), sqrt(1 - area))
    
    # Calculate Distance
    radius = 6371
    distance = central_angle * radius
    
    # Return distance
    return abs(round(distance, 2))

# Convert it to UDF
udfGetDistance = udf(getDistance)

adIdDistance = sampleDf.withColumn("distance", \
    udfGetDistance(lit(billboardCoordinates[0]), lit(billboardCoordinates[1]),
                   sampleDf.latitude, sampleDf.longitude))

# Show distance
adIdDistance.show()

# Filter IDFAs (advertisement_id) that are within 1km radius of billboard
allAdId = adIdDistance.filter(adIdDistance.distance <= 1)

# Total: 834648
# Unique: 11691
#totalCount = allAddId.select("advertisement_id").count()
#uniqueCount = allAddId.select("advertisement_id").distinct().count()

# Drop distance column
allAdId = allAdId.drop("distance")

# Create date column for indexing and aggregation
allAdIdDate = allAdId.select("*", col("date_time").cast("date").alias("date"))

# Create hour column for indexing and aggregation
allAdIdHour = allAdIdDate.select("*", hour("date_time").cast("int").alias("broadcast_hour"))

#bukitBintang = allAdIdHour.toPandas()
#bukitBintang.to_csv("bukit_bintang.csv", index = False)

# Filter IDFAs (advertisement_id) that are from February 1st and horizontal
# accuracy are within 30.00m or not null
allAdIdFilter = allAdIdHour.filter((allAdIdHour.date == "2019-02-01") & 
        ((allAdIdHour.horizontal_accuracy <= 30) & 
             (allAdIdHour.horizontal_accuracy.isNotNull())))

# Drop horizontal accuracy since it's redundant for future analysis
allAdIdClean = allAdIdFilter.drop("horizontal_accuracy", "date", "latitude", "longitude")
allAdId = allAdIdClean.toPandas()

# Aggregate advertisement_id by hour
allAdIdGroup = allAdId.groupBy("broadcast_hour", "advertisement_id").agg(count("advertisement_id"))

bukitBintang = allAdIdAgg.toPandas()
bukitBintang.to_csv("bukit_bintang.csv", index = False)

totalAdId = allAdIdAgg.select("advertisement_id").count()   # 8,543, 35608
totalUniqueAdId = allAdIdAgg.select("advertisement_id").distinct().count()  #3,310, 12876