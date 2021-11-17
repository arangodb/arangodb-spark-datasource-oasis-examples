import com.arangodb.ArangoDBException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OasisAndSpark {
    public static void main(String[] args) {
        // Enter your Database Credentials here
        String databaseName = "_system"; // replace with <YOUR_DATABASE_NAME>
        String username = "root"; // can replace with a different user account if you have one
        String password = "<get-from-dashboard>";
        String endpointsList = "<ENDPOINTS_HERE>"; // If you have a cluster, you can use multiple endpoints with comma separators
        String encodedCA = "<YOUR_CA_CERT_HERE>";

        // Set up Spark Datasource Options
        Map<String, String> baseArangoDBDataSourceOptions = Stream.of(new String[][] {
                { "database", databaseName },
                { "user", username },
                { "password", password },
                { "endpoints", endpointsList },
                { "ssl.enabled", "true" },
                { "ssl.cert.value", encodedCA }
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                //.config("spark.jars.packages", "com.arangodb:arangodb-spark-datasource-3.1_2.12:0.1.0-SNAPSHOT")
                .getOrCreate();

        // Spark is lazy, so to connect the datasource to ArangoDB we can seed a dataframe and write it to the database
        String dummyCollectionName = "dummyCol";

        try {
            spark.read()
                    .format("org.apache.spark.sql.arangodb.datasource")
                    .options(baseArangoDBDataSourceOptions)
                    .option("table", dummyCollectionName)
                    .load();
        } catch (ArangoDBException e) {
            // If the "dummyCol" collection does not exist, it will return a 1203 error code
            // https://www.arangodb.com/docs/stable/appendix-error-codes.html#1203
            if (e.getErrorNum() == 1203) {
                System.out.println("Able to connect!");
            } else {
                throw e;
            }
        }

        spark.stop();
    }
}
