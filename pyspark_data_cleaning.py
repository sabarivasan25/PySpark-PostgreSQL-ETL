from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, count, avg, max, min, round
)
from getpass import getpass

# PostgreSQL Configuration
password = getpass("Enter PostgreSQL password: ")

jdbc_jar = r"D:\PySpark_Assessment\jars\postgresql-42.7.14.jar"
jdbc_url = "jdbc:postgresql://localhost:5432/pyspark_postgres_db"

properties = {
    "user": "postgres",
    "password": password,
    "driver": "org.postgresql.Driver"
}

spark = None

try:
    # 1. Create Spark Session
    spark = (
        SparkSession.builder
        .appName("PySpark_Data_Cleaning")
        .master("local[*]")
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("Spark Session Started Successfully")

    # 2. Read Employee Data
    employees_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.employees",
        properties=properties
    )

    print("\nOriginal Employee Data:")
    employees_df.show()

    print("Original Record Count:", employees_df.count())

    # 3. Remove NULL values
    cleaned_df = employees_df.dropna(
        subset=[
            "employee_id",
            "employee_name",
            "salary",
            "department_id"
        ]
    )

    # 4. Trim employee names
    cleaned_df = cleaned_df.withColumn(
        "employee_name",
        trim(col("employee_name"))
    )

    # 5. Remove empty names
    cleaned_df = cleaned_df.filter(
        col("employee_name") != ""
    )

    # 6. Remove duplicate employee IDs
    cleaned_df = cleaned_df.dropDuplicates(
        ["employee_id"]
    )

    print("\nCleaned Employee Data:")
    cleaned_df.show()

    print("Cleaned Record Count:", cleaned_df.count())

    # 7. Read Department Data
    departments_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.departments",
        properties=properties
    )

    # 8. Join employee and department data
    joined_df = cleaned_df.join(
        departments_df,
        "department_id",
        "inner"
    )

    # 9. Department Salary Analysis
    summary_df = (
        joined_df
        .groupBy("department_id", "department_name")
        .agg(
            count("employee_id").alias("employee_count"),
            round(avg("salary"), 2).alias("average_salary"),
            max("salary").alias("maximum_salary"),
            min("salary").alias("minimum_salary")
        )
    )

    print("\nDepartment Salary Analysis:")
    summary_df.show()

    # 10. Write results to PostgreSQL
    summary_df.write.jdbc(
        url=jdbc_url,
        table="public.department_salary_cleaned",
        mode="overwrite",
        properties=properties
    )

    print("\nCleaned Data Successfully Written!")

    # 11. Verify PostgreSQL output
    result_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.department_salary_cleaned",
        properties=properties
    )

    result_df.show()

    print("Total Departments:", result_df.count())

except Exception as error:
    print("\nError:", error)

finally:
    if spark is not None:
        spark.stop()
        print("\nSpark Session Stopped Successfully")
