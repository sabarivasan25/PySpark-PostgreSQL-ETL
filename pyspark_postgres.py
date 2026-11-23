
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    avg,
    max,
    min,
    round
)
from getpass import getpass

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
        .appName("PostgreSQL_Data_Engineering")
        .master("local[*]")
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("\nSpark Started:", spark.version)

    # 2. Read Employees
    employees_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.employees",
        properties=properties
    )

    # 3. Read Departments
    departments_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.departments",
        properties=properties
    )

    print("\nOriginal Employee Data:")
    employees_df.show()

    # 4. Join Employees and Departments
    joined_df = employees_df.join(
        departments_df,
        "department_id",
        "inner"
    )

    # 5. Department-wise Salary Analysis
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

    # 6. Write Results to PostgreSQL
    summary_df.write.jdbc(
        url=jdbc_url,
        table="public.department_salary_summary",
        mode="overwrite",
        properties=properties
    )

    print("\nData successfully written to PostgreSQL!")

    # 7. Read Back and Verify
    result_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.department_salary_summary",
        properties=properties
    )

    print("\nFinal PostgreSQL Summary Table:")
    result_df.show()

    print("Total Departments:", result_df.count())

except Exception as error:
    print("\nError:", error)

finally:
    if spark is not None:
        spark.stop()
        print("\nSpark Session Stopped.")

