# Import the ready-made Spark session if you are running with pyspark
from pyspark.shell import spark

# Import SparkSession class
from pyspark.sql import SparkSession

# Import pyspark
import pyspark

# Import all data types like IntegerType, StringType, DoubleType, etc.
from pyspark.sql.types import *

# This block means: run the code below only when this file is executed directly
if __name__ == "__main__":

    # Read the CSV file normally
    # Spark will read all columns as string by default
    # It will also give default column names like _c0, _c1, _c2 ...
    df = spark.read.csv("file:///home/takeo/zipcodes.csv")

    # Read the CSV file again, but this time use the first row as column names
    # Example: RecordNumber, Zipcode, City, State ...
    df2 = spark.read.option("header", True).csv("file:///home/takeo/zipcodes.csv")

    # Read the CSV file and tell Spark that the separator is a comma
    # This is useful when working with CSV files
    df3 = spark.read.options(delimiter=',').csv("file:///home/takeo/zipcodes.csv")

    # Print the structure of the DataFrame
    # This shows column names and data types
    df3.printSchema()

    # Show the data inside the DataFrame
    df3.show()

    # Read the CSV file and ask Spark to guess the data types automatically
    # For example:
    # numbers -> integer/double
    # true/false -> boolean
    df4 = spark.read.options(inferSchema='True', delimiter=',') \
        .csv("file:///home/takeo/zipcodes.csv")

    # Print the schema after Spark guesses the column types
    df4.printSchema()

    # Show the data
    df4.show()

    # Another way to do the same thing:
    # infer schema and set delimiter
    df4 = spark.read.option("inferSchema", True) \
        .option("delimiter", ",") \
        .csv("file:///home/takeo/zipcodes.csv")

    # Print schema
    df4.printSchema()

    # Show data
    df4.show()

    # Read the CSV file with:
    # 1. header = True -> use first row as column names
    # 2. inferSchema = True -> let Spark guess data types
    # 3. delimiter = ',' -> comma separated values
    df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
        .csv("file:///home/takeo/zipcodes.csv")

    # Print schema
    df3.printSchema()

    # Show data
    df3.show()

    # Create our own custom schema manually
    # This means we tell Spark exactly what each column type should be
    schema = StructType() \
        .add("RecordNumber", IntegerType(), True) \
        .add("Zipcode", IntegerType(), True) \
        .add("ZipCodeType", StringType(), True) \
        .add("City", StringType(), True) \
        .add("State", StringType(), True) \
        .add("LocationType", StringType(), True) \
        .add("Lat", DoubleType(), True) \
        .add("Long", DoubleType(), True) \
        .add("Xaxis", IntegerType(), True) \
        .add("Yaxis", DoubleType(), True) \
        .add("Zaxis", DoubleType(), True) \
        .add("WorldRegion", StringType(), True) \
        .add("Country", StringType(), True) \
        .add("LocationText", StringType(), True) \
        .add("Location", StringType(), True) \
        .add("Decommisioned", BooleanType(), True) \
        .add("TaxReturnsFiled", StringType(), True) \
        .add("EstimatedPopulation", IntegerType(), True) \
        .add("TotalWages", IntegerType(), True) \
        .add("Notes", StringType(), True)

    # Read the CSV file again using the custom schema we created above
    # header = True means first row is the column name
    df_with_schema = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load("file:///home/takeo/zipcodes.csv")

    # Print the schema of this DataFrame
    df_with_schema.printSchema()

    # Show the data
    df_with_schema.show()

    # Save the DataFrame as CSV
    # mode("overwrite") means:
    # if the folder already exists, replace it
    df_with_schema.write.mode('overwrite').csv("file:///tmp/spark_output/zipcodes")

    # Create sample data for Parquet, ORC and JSON examples
    data = [
        ("James ", "", "Smith", "36636", "M", 3000),
        ("Michael ", "Rose", "", "40288", "M", 4000),
        ("Robert ", "", "Williams", "42114", "M", 4000),
        ("Maria ", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
    ]

    # Column names for the sample data
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    # Create a DataFrame from the sample data
    df = spark.createDataFrame(data, columns)

    # Save the DataFrame in Parquet format
    # Parquet is a column-based format and is very common in Spark
    df.write.mode("overwrite").parquet("file:///tmp/output/people.parquet")

    # Read the Parquet file back into a DataFrame
    parDF = spark.read.parquet("file:///tmp/output/people.parquet")

    # Print schema of Parquet DataFrame
    parDF.printSchema()

    # Show Parquet data
    parDF.show()

    # Create a temporary SQL table from the Parquet DataFrame
    # This lets us run SQL queries on it
    parDF.createOrReplaceTempView("ParquetTable")

    # Run SQL query:
    # select all rows where salary is 4000 or more
    parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

    # Show SQL query result
    parkSQL.show()

    # Save the Parquet DataFrame in ORC format
    # ORC is another big-data file format
    parDF.write.mode("overwrite").orc("file:///tmp/orc/data.orc")

    # Read the ORC file back into a DataFrame
    df = spark.read.orc("file:///tmp/orc/data.orc")

    # Print schema of ORC DataFrame
    df.printSchema()

    # Show ORC data
    df.show()

    # Create a temporary SQL table from ORC DataFrame
    df.createOrReplaceTempView("ORCTable")

    # Run SQL query on ORC data:
    # show firstname and dob where salary is 4000 or more
    orcSQL = spark.sql("select firstname,dob from ORCTable where salary >= 4000 ")

    # Show SQL result
    orcSQL.show()

    # Save the Parquet DataFrame in JSON format
    parDF.write.mode("overwrite").json("file:///tmp/json/data.json")

    # Read the JSON file back into a DataFrame
    df = spark.read.json("file:///tmp/json/data.json")

    # Print schema of JSON DataFrame
    df.printSchema()

    # Show JSON data
    df.show()
