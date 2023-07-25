from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Test") \
    .getOrCreate()

# Hide all warning messages and only show errors
spark.sparkContext.setLogLevel('ERROR')


# Create a simple DataFrame
df = spark.createDataFrame([(1, "John Doe", 25),
                            (2, "Jane Doe", 23),
                            (3, "Mike Johnson", 30)], 
                           ["Id", "Name", "Age"])

# Perform a simple operation (count the number of rows)
print("The DataFrame has {} rows.".format(df.count()))
