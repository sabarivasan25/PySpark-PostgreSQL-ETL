
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    spark = (
        SparkSession.builder
        .appName("PostgreSQL_Incremental_ETL")
        .master("local[*]")
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("Spark Session Started")

    # 1. Read watermark
    watermark_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.etl_watermark",
        properties=properties
    )

    watermark = (
        watermark_df
        .filter(
            col("pipeline_name") ==
            "employee_incremental_pipeline"
        )
        .select("last_employee_id")
        .first()
    )

    last_id = watermark["last_employee_id"]

    print("Previous Watermark:", last_id)

    # 2. Read only new employees
    employees_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.employees",
        properties=properties
    )

    new_employees_df = employees_df.filter(
        col("employee_id") > last_id
    )

    print("\nNew Employee Records:")
    new_employees_df.show()

    new_count = new_employees_df.count()

    if new_count == 0:
        print("No new records found.")

    else:
        # 3. Clean new records
        cleaned_df = (
            new_employees_df
            .dropna(subset=[
                "employee_id",
                "employee_name",
                "salary",
                "department_id"
            ])
            .dropDuplicates(["employee_id"])
        )

        # 4. Append to target table
        cleaned_df.write.jdbc(
            url=jdbc_url,
            table="public.employee_incremental_target",
            mode="append",
            properties=properties
        )

        # 5. Update watermark after successful load
        new_max_id = (
            cleaned_df
            .agg({"employee_id": "max"})
            .first()[0]
        )

        if new_max_id is not None:
            import psycopg2

            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="pyspark_postgres_db",
                user="postgres",
                password=password
            )

            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE public.etl_watermark
                SET last_employee_id = %s
                WHERE pipeline_name = %s
                """,
                (
                    new_max_id,
                    "employee_incremental_pipeline"
                )
            )

            conn.commit()
            cursor.close()
            conn.close()

        print("\nIncremental Load Successful!")
        print("New Records Loaded:", new_count)
        print("Updated Watermark:", new_max_id)

    # 6. Verify target
    target_df = spark.read.jdbc(
        url=jdbc_url,
        table="public.employee_incremental_target",
        properties=properties
    )

    print("\nIncremental Target Table:")
    target_df.show()

    print("Total Target Records:", target_df.count())

except Exception as error:
    print("\nETL Error:", error)

finally:
    if spark is not None:
        spark.stop()
        print("Spark Session Stopped")

