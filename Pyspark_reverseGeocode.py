from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the UDF for reverse geocoding
def reverse_geocode(coordinates):
    # Your reverse geocoding logic goes here
    # Replace this with your actual reverse geocoding implementation
    return {'name': 'City1', 'admin1': 'State1', 'admin2': 'District1'}

# Register the UDF with the return type as struct
reverse_geocode_udf = udf(reverse_geocode, StructType([
    StructField('name', StringType(), nullable=False),
    StructField('admin1', StringType(), nullable=False),
    StructField('admin2', StringType(), nullable=False)
]))

# Add the UDF to the DataFrame and extract individual fields
df_with_geocoded_data = pdVehicles.withColumn(
    'data', reverse_geocode_udf(struct('gpsLatitude', 'gpslongitude'))
).withColumn(
    'City_Town', col('data').getField('name')
).withColumn(
    'State', col('data').getField('admin1')
).withColumn(
    'District', col('data').getField('admin2')
)

# Drop the unnecessary columns
df_with_geocoded_data = df_with_geocoded_data.drop('data')

# Show the resulting DataFrame
df_with_geocoded_data.show()
