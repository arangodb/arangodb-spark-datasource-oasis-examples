# Please note: PySpark 2.4 is known to have issues running on Python versions higher than 3.7.
import os

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession


def main():
    # SUBMIT_ARGS = '--packages com.arangodb:arangodb-spark-datasource-2.4_2.11:0.2.0-SNAPSHOT --repositories="https://oss.sonatype.org/content/repositories/snapshots" pyspark-shell'
    # os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    databaseName = "_system" # replace with <YOUR_DATABASE_NAME>
    username = "root" # can replace with a different user account if you have one
    password = "<get-from-dashboard>"
    endpointsList = "<ENDPOINTS_HERE>" # If you have a cluster, you can use multiple endpoints with comma separators
    encodedCA = "<YOUR_CA_CERT_HERE>"

    # Set up Spark Datasource Options
    baseArangoDBDataSourceOptions = {
        "database": databaseName,
        "user": username,
        "password": password,
        "endpoints": endpointsList,
        "ssl.cert.value": encodedCA,
        "ssl.enabled": "true"
    }

    spark = SparkSession.builder \
        .config("spark.jars.packages", "com.arangodb:arangodb-spark-datasource-2.4_2.11:0.2.0-SNAPSHOT") \
        .appName("oasisConnection") \
        .master("local[1]") \
        .getOrCreate()

    dummyCollectionName = "dummyCol"

    try:
        spark.read \
            .format("org.apache.spark.sql.arangodb.datasource") \
            .options(**baseArangoDBDataSourceOptions) \
            .option("table", dummyCollectionName) \
            .load()
    except Py4JJavaError as e:
        # If the "dummyCol" collection does not exist, it will return a 1203 error code
        # https://www.arangodb.com/docs/stable/appendix-error-codes.html#1203
        if "1203" in str(e.java_exception):
            print("Able to connect!")
        else:
            raise e

    spark.stop()


if __name__ == "__main__":
    main()
