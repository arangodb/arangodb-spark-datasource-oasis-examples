import com.arangodb.ArangoDBException
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object main {
  def main(args: Array[String]): Unit = {
    // Enter your Database Credentials here
    val databaseName = "_system" // replace with <YOUR_DATABASE_NAME>
    val username = "root" // can replace with a different user account if you have one
    val password = "<get-from-dashboard>"
    val endpointsList = "<ENDPOINTS_HERE>" // If you have a cluster, you can use multiple endpoints with comma separators
    val encodedCA = "<YOUR_CA_CERT_HERE>"

    // Set up Spark Datasource Options
    val baseArangoDBDataSourceOptions = Map(
      "database" -> databaseName,
      "user" -> username,
      "password" -> password,
      "endpoints" -> endpointsList,
      "ssl.cert.value" -> encodedCA,
      "ssl.enabled" -> "true"
    )

    val spark = SparkSession.builder
      .master("local[1]")
      //.config("spark.jars.packages", "com.arangodb:arangodb-spark-datasource-3.1_2.12:0.2.0-SNAPSHOT")
      .getOrCreate()


    // Spark is lazy, so to connect the datasource to ArangoDB we can seed a dataframe and write it to the database
    val dummyCollectionName = "dummyCol"

    try {
      spark.read
        .format("org.apache.spark.sql.arangodb.datasource")
        .options(baseArangoDBDataSourceOptions)
        .option("table", dummyCollectionName)
        .load()
    } catch {
      // If the "dummyCol" collection does not exist, it will return a 1203 error code
      // https://www.arangodb.com/docs/stable/appendix-error-codes.html#1203
      case e: ArangoDBException if e.getErrorNum == 1203 => println("Able to connect!")
    }

    spark.stop()
  }
}
