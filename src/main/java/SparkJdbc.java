import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by dmagda on 8/10/17.
 */
public class SparkJdbc {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("SparkJdbc")
            .master("local[*]")
            .getOrCreate();

        String url = "jdbc:ignite:thin://127.0.0.1/";

        Properties prop = new Properties();

        prop.setProperty("fetchSize", "100");


        // "Pushdown" query that will be executed directly over Ignite's dataset.
        // See: https://docs.databricks.com/spark/latest/data-sources/sql-databases.html#pushdown-query-to-database-engine
        String query = "(SELECT p.name as person, c.name as city " +
            "FROM person p, city c  WHERE p.city_id = c.id) res_frame";

        //String query = "person";


        Dataset<Row> df = spark.read().jdbc(url, query, prop);

        df.show();
    }
}
