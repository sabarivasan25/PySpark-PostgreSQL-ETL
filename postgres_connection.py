import psycopg2
from getpass import getpass

password = getpass("Enter PostgreSQL password: ")

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="pyspark_postgres_db",
        user="postgres",
        password=password
    )

    print("PostgreSQL Connected Successfully!")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees")

    rows = cursor.fetchall()

    for row in rows:
        print(row)

    print("Total Employees:", len(rows))

    cursor.close()
    conn.close()

except Exception as e:
    print("Error:", e)


from pyspark.sql import SparkSession
from getpass import getpass

# PostgreSQL credentials
password = getpass("Enter PostgreSQL password: ")

# JDBC Driver location
jdbc_jar = r"D:\PySpark_Assessment\jars\postgresql-42.7.14.jar"

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("PySpark_PostgreSQL_Integration")
    .master("local[*]")
    .config("spark.jars", jdbc_jar)
    .getOrCreate()
)

print("Spark Version:", spark.version)
print("Spark Session Created Successfully!")

# PostgreSQL connection details
jdbc_url = "jdbc:postgresql://localhost:5432/pyspark_postgres_db"

connection_properties = {
    "user": "postgres",
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Read PostgreSQL table into Spark DataFrame
employees_df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="public.employees",
        properties=connection_properties
    )
)

# Display DataFrame
print("\nEmployee Data from PostgreSQL:")

employees_df.show()

# Display Schema
print("\nDataFrame Schema:")

employees_df.printSchema()

# Count records
print("\nTotal Employees:", employees_df.count())

print("\nPostgreSQL JDBC Integration Successful!")

spark.stop()
