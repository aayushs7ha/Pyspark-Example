import numpy as np
from pyspark.sql.functions import *
import os
import sys
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from functools import reduce
from pyspark.sql import DataFrame
import pyspark
#sc=pyspark.SparkContext()
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
#import sys
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lead, lag, col ,datediff, to_date, lit, size , length , year, month, dayofmonth, from_unixtime, unix_timestamp, desc, asc, rank, min , expr, trim, date_format, avg
import datetime
from datetime import datetime
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType, StructField, NumericType
from pyspark.sql.functions import isnan
from pyspark.sql.functions import udf
from io import BytesIO
#import boto3
import math
from pyspark.sql import Row
#import pyarrow as pa
import pandas as pd
from datetime import datetime, timedelta
#from time import gmtime, strftime ,datetime
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import count, min,max
from pyspark.sql.functions import col, when
import geohash2 as pgh
import pygeohash as pgh2
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")



#df=spark.read.parquet("s3://cv-cvp-fleetascent/vehicle360/BS6/aggregation_table/Telemetry_Event_FMS_Data/eventdate=2023-08-25/")
# Read data for the month of December
df = spark.read.parquet("s3://cv-cvp-fleetascent/vehicle360/BS6/aggregation_table/Telemetry_Event_FMS_Data/eventdate=2023-12-*")
columns_to_keep = ['vehicleId', 'eventDateTime', 'gpsLatitude', 'gpslongitude', 'RashTurning', 'Idling', 'OverSpeeding', 'HarshAcceleration', 'HarshBrake']
df_filtered = df[columns_to_keep]

# Geohash 
udf1 = F.udf(lambda x,y: pgh.encode(x,y,precision=7)) # Check counts with g7
df1= df_filtered.select('vehicleId','eventDateTime','gpsLatitude','gpslongitude',
                'rashturning','overspeeding','harshacceleration','harshbrake',
                udf1('gpsLatitude','gpslongitude').alias('geohash_7'))

udf2 = F.udf(lambda x,y: pgh.encode(x,y,precision=5)) # Check counts with g5
df1= df1.select('vehicleId','eventDateTime','gpsLatitude','gpslongitude',
                'rashturning','overspeeding','harshacceleration','harshbrake','geohash_7',
                udf2('gpsLatitude','gpslongitude').alias('geohash_5'))
 df1.show(3)

# UDF to decode geohash_7
import pygeohash as pgh2
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, DoubleType,FloatType
udf2 = F.udf(lambda x: pgh2.decode(x),ArrayType(FloatType()))

df2 = df1.select('vehicleId','eventDateTime','gpsLatitude','gpslongitude',
'rashturning','overspeeding','harshacceleration','harshbrake',
'geohash_7','geohash_5',udf2('geohash_7').alias('decodedVal'))

df2.show(4)

from pyspark.sql.functions import split
from pyspark.sql.functions import col
# split the decoded value into latitude and longitude columns
df3 = df2.withColumn("Latitude", col("decodedVal")[0].cast("float"))
df3 = df3.withColumn("Longitude", col("decodedVal")[1].cast("float"))
df3.show(3)

+-----------------+-------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+
|        vehicleId|eventDateTime|gpsLatitude|gpslongitude|rashturning|overspeeding|harshacceleration|harshbrake|geohash_7|    decodedVal|Latitude|Longitude|
+-----------------+-------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+
|MAT785001P7B05005| 202308231600| 26.5935952|  93.7080384|          0|           0|                0|         0|  whdxk3h|[26.59, 93.71]|   26.59|    93.71|
|MAT506327M8K14574| 202308232240| 22.5655728|  88.2128448|          0|           0|                0|         0|  tun8r7f|[22.57, 88.21]|   22.57|    88.21|
|MAT785004L7G04491| 202308230100| 18.7208672|  84.3749568|          0|           0|                0|         0|  tg7fxbp|[18.72, 84.37]|   18.72|    84.37|
+-----------------+-------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+

# Convert eventDateTime to datetime
df3_with_datetime = df3.withColumn("eventDateTime", unix_timestamp(col("eventDateTime"), "yyyyMMddHHmm").cast(TimestampType()))
df3_with_datetime.show(truncate=False)
df3_with_datetime.show(3)
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+
|        vehicleId|      eventDateTime|gpsLatitude|gpslongitude|rashturning|overspeeding|harshacceleration|harshbrake|geohash_7|    decodedVal|Latitude|Longitude|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+
|MAT785001P7B05005|2023-08-23 16:00:00| 26.5935952|  93.7080384|          0|           0|                0|         0|  whdxk3h|[26.59, 93.71]|   26.59|    93.71|
|MAT506327M8K14574|2023-08-23 22:40:00| 22.5655728|  88.2128448|          0|           0|                0|         0|  tun8r7f|[22.57, 88.21]|   22.57|    88.21|
|MAT785004L7G04491|2023-08-23 01:00:00| 18.7208672|  84.3749568|          0|           0|                0|         0|  tg7fxbp|[18.72, 84.37]|   18.72|    84.37|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------------+--------+---------+
df4= df3_with_datetime.drop("decodedVal")
df4 = df4.withColumn("coordinates", concat(col("Latitude"), lit(", "), col("Longitude")))
df4.show(3)
 +-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+
|        vehicleId|      eventDateTime|gpsLatitude|gpslongitude|rashturning|overspeeding|harshacceleration|harshbrake|geohash_7|Latitude|Longitude|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+
|MAT785001P7B05005|2023-08-23 16:00:00| 26.5935952|  93.7080384|          0|           0|                0|         0|  whdxk3h|   26.59|    93.71|
|MAT506327M8K14574|2023-08-23 22:40:00| 22.5655728|  88.2128448|          0|           0|                0|         0|  tun8r7f|   22.57|    88.21|
|MAT785004L7G04491|2023-08-23 01:00:00| 18.7208672|  84.3749568|          0|           0|                0|         0|  tg7fxbp|   18.72|    84.37|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+

from pyspark.sql.functions import date_format

# Assuming your DataFrame is named df4
df4 = df4.withColumn("eventMonth", date_format(df4["eventDateTime"], "MMMM"))
# Show the result
df4.select("eventDateTime", "eventMonth").show(3)


# JOIN WITH TML ASSET 

df_tmlAsset=spark.read.parquet("s3://cv-cvp-fleetascent/vehicle360/BS6/CRM/gendatamodeldb/TML_ASSET/")
selected_columns = ['asset_number', 'lob', 'pl', 'ppl','dlr_region','dlr_state','dlr_org_city','dlr_district']
tml_asset = df_tmlAsset[selected_columns].drop_duplicates(subset=['asset_number'])
tml_asset.show(4)


+--------------------+-------+------------------+--------------------+
|        asset_number|    lob|                pl|                 ppl|
+--------------------+-------+------------------+--------------------+
| \t MA10000WRBG97676|CV NTML|               BMT|               M & M|
|\t MBX0003AHJA562891|CV NTML|         Ape Truck|    Piaggio Vehicles|
| \tMA1ZC4TNKL3F92927|CV NTML|Mahindra Bolero PU|Mahindra Trucks &...|
|\tMA2ERLF1S0025695\t|CV NTML|        EECO Cargo|        Maruti Udyog|
+--------------------+-------+------------------+--------------------+

tml_asset = tml_asset.withColumnRenamed("dlr_region", "Region")\
               .withColumnRenamed("dlr_state", "State")\
               .withColumnRenamed("dlr_org_city", "City")\
               .withColumnRenamed("dlr_district", "District")

tml_asset= tml_asset.withColumn("asset_number", trim(tml_asset["asset_number"]))
tml_asset_cleaned = tml_asset.withColumn("asset_number", trim(tml_asset["asset_number"]))
# Joining DataFrames based on 'vehicleId' and 'asset_number'
df4_with_asset = df4.join(tml_asset_cleaned, df4.vehicleId == tml_asset_cleaned.asset_number, "left")

df4_with_asset.show(3)
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+-----------------+---------+------------------+--------------+
|        vehicleId|      eventDateTime|gpsLatitude|gpslongitude|rashturning|overspeeding|harshacceleration|harshbrake|geohash_7|Latitude|Longitude|     asset_number|      lob|                pl|           ppl|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+-----------------+---------+------------------+--------------+
|MAT783018M3A00032|2023-08-23 08:20:00| 23.6599728|   85.367072|          0|           0|                0|         0|  tuhwwe4|   23.66|    85.37|MAT783018M3A00032|HCV Const|SIGNA 4225.TK. FBV|MAV Tippers 42|
|MAT784053MFP04116|2023-08-23 07:50:00|   23.28252|  79.9908736|          0|           0|                0|         0|  tu0uknr|   23.28|    79.99|MAT784053MFP04116|    Buses|           LPO10.2|     ICV Buses|
|MAT784053NFG07174|2023-08-23 20:50:00| 20.9650096|  86.0692032|          0|           0|                0|         0|  tgtpq89|   20.97|    86.07|MAT784053NFG07174|    Buses|       LPO10.2 FBV|     ICV Buses|
+-----------------+-------------------+-----------+------------+-----------+------------+-----------------+----------+---------+--------+---------+-----------------+---------+------------------+--------------+

df4_with_asset_raw = df4_with_asset.filter(
    (df4_with_asset.Latitude != -90) & (df4_with_asset.Latitude != 90) &
    (df4_with_asset.Longitude != -180) & (df4_with_asset)
)
#df4_with_asset = df4_with_asset.dropna(subset=['vehicleId'])

# Remove incorrect Latitude - Longitude values.
#final_df = df4_with_asset_raw.filter((df4_with_asset_raw.geohash_7 != 'zzzzzzz') & (df4_with_asset_raw.geohash_7 != '7zzzzzz'))


#====================================================================================================================================================================================


from pyspark.sql.functions import col, sum as _sum

# Filter the original DataFrame to create four separate DataFrames
rashturning_df = df4_with_asset.filter((col("rashturning") > 0) & (col("overspeeding") == 0) & (col("harshacceleration") == 0) & (col("harshbrake") == 0))
overspeeding_df = df4_with_asset.filter((col("rashturning") == 0) & (col("overspeeding") > 0) & (col("harshacceleration") == 0) & (col("harshbrake") == 0))
harshacceleration_df = df4_with_asset.filter((col("rashturning") == 0) & (col("overspeeding") == 0) & (col("harshacceleration") > 0) & (col("harshbrake") == 0))
harshbrake_df = df4_with_asset.filter((col("rashturning") == 0) & (col("overspeeding") == 0) & (col("harshacceleration") == 0) & (col("harshbrake") > 0))

# Aggregate VinCount in a temporary DataFrame
vincount_df = df4_with_asset.groupBy("vehicleId").agg(_sum("VinCount").alias("totalVinCount"))
vincount_df = vincount_df.filter(col("totalVinCount") > 10)

# Join the filtered DataFrames with the aggregated VinCount DataFrame
rashturning_df = rashturning_df.join(vincount_df, "vehicleId")
overspeeding_df = overspeeding_df.join(vincount_df, "vehicleId")
harshacceleration_df = harshacceleration_df.join(vincount_df, "vehicleId")
harshbrake_df = harshbrake_df.join(vincount_df, "vehicleId")

from pyspark.sql.functions import lit

# Add an indicator column to each DataFrame
rashturning_df = rashturning_df.withColumn("event_type", lit("rashturning_event"))
overspeeding_df = overspeeding_df.withColumn("event_type", lit("overspeeding_event"))
harshacceleration_df = harshacceleration_df.withColumn("event_type", lit("harshacceleration_event"))
harshbrake_df = harshbrake_df.withColumn("event_type", lit("harshbrake_event"))

# Concatenate the four DataFrames
concatenated_df = rashturning_df.union(overspeeding_df).union(harshacceleration_df).union(harshbrake_df)

# Show the first few rows of the concatenated DataFrame
concatenated_df.show()
#===============================================================================================
# Final DataFrame 
final_df=df4_with_asset_raw.select('vehicleId','eventMonth','geohash_7','geohash_5','Latitude', 'Longitude','eventDateTime','LOB','PL','PPL','Region', 'State', 'City', 'District','rashturning','overspeeding','harshacceleration','harshbrake').groupBy('geohash_7','geohash_5','vehicleId','Latitude', 'Longitude','eventMonth','eventDateTime','LOB','PL','PPL','Region', 'State', 'City', 'District','rashturning','overspeeding','harshacceleration','harshbrake').agg(countDistinct('vehicleId'))

'''
final_df = final_df.filter(
	(final_df.Latitude >= -90) & (final_df.Latitude <= 90) &
		#(final_df.Longitude >= -180) & (final_df.Longitude <= 180)
	)
'''
final_df.show(3)

final_df=final_df.withColumnRenamed('count(vehicleId)', 'VinCount')

final_df.show(3)

final_df = final_df.withColumn('eventdate_1',col('eventDateTime').cast('string'))


'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# Create a Spark session
spark = SparkSession.builder.appName("GeoHashAggregation").getOrCreate()


# List of columns for which you want to calculate distinct counts
event_columns = ["rashturning", "overspeeding", "harshacceleration", "harshbrake"]

# Group by geohash_7 and geohash_5, and calculate distinct counts for each event
final_agg_df = final_df.groupBy("geohash_7", "geohash_5").agg(
    *[countDistinct(col).alias(f"{col}_count") for col in event_columns]
)

gh = final_df.groupBy("geohash_7").count()
gh.show()

'''



final_df = final_df.withColumn('eventdate_1',col('eventDateTime').cast('string')).write.mode('overwrite').option('path','s3://cv-cvp-fleetascent/vehicle360/BS6/Capacity_Estimation_Analysis/HeatMap26Aug').saveAsTable('capacity_estimation_analysis.HeatMap1Day26082023')



'''
# Use show create table to find the location and schema of the data

presto:capacity_estimation_analysis> show create table heatmap1day23082023_1;
                                                   Create Table
-------------------------------------------------------------------------------------------------------------------
 CREATE TABLE hive.capacity_estimation_analysis.heatmap1day23082023_1 (
    "geohash_7" varchar,
    "latitude" real,
    "longitude" real,
    "eventdatetime" timestamp,
    "lob" varchar,
    "pl" varchar,
    "ppl" varchar,
    "vincount" bigint,
    "eventdate_1" varchar
 )
 WITH (
    external_location = 's3://cv-cvp-fleetascent/vehicle360/BS6/Capacity_Estimation_Analysis/HeatMap26Aug/HeatMap1Day26082023',
    format = 'PARQUET'
 )

'''
# Read parquet file from the location 

df=spark.read.parquet("s3://cv-cvp-fleetascent/vehicle360/BS6/Capacity_Estimation_Analysis/HeatMap26Aug")


df.registerTempTable('temp')
df.printSchema()
df1=spark.sql("select * from temp limit 25000")
df1.count()
df1.toPandas().to_csv("/home/sinha.ttl/HeatMapsample.csv")

[sinha.ttl@ip-172-16-243-170 ~]$ ll
total 988
-rw-rw-r-- 1 sinha.ttl sinha.ttl 1008339 Aug 29 06:10 HeatMap1Day23082023_sample.csv

# Go to WinScp

'''

# Coordinates 
df = df.withColumn(
    "Coordinate",
    concat(col("Latitude"), lit(", "), col("Longitude"))
)


final_df.registerTempTable('temp')
final_df.printSchema()
df1=spark.sql("select * from temp limit 25000")
df1.count()
df1.toPandas().to_csv("/home/sinha.ttl/HeatMap1Day26082023_1_sample.csv")
'''







============================================================
SELECT geohash_7, COUNT(*) as count
FROM heatmap1day26082023
GROUP BY geohash_7
ORDER BY count DESC;



min_max_df = final_df.agg(
    min("Latitude").alias("MinLatitude"),
    max("Latitude").alias("MaxLatitude"),
    min("Longitude").alias("MinLongitude"),
    max("Longitude").alias("MaxLongitude")
)
