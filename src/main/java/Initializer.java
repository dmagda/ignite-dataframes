import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by dmagda on 8/10/17.
 */
public class Initializer {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Register JDBC driver
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Open JDBC connection
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

        // Create database tables
        try (Statement stmt = conn.createStatement()) {

            // Create table based on REPLICATED template
            stmt.executeUpdate("CREATE TABLE city (" +
                " id LONG PRIMARY KEY, name VARCHAR) " +
                " WITH \"template=replicated\"");

            // Create table based on PARTITIONED template with one backup
            stmt.executeUpdate("CREATE TABLE person (" +
                " id LONG, name VARCHAR, city_id LONG, " +
                " PRIMARY KEY (id, city_id)) " +
                " WITH \"backups=1, affinityKey=city_id\"");
        }

        // Create indexes
        try (Statement stmt = conn.createStatement()) {

            // Create an index on the city table
            stmt.executeUpdate("CREATE INDEX idx_city_name ON city (name)");

            // Create an index on the person table
            stmt.executeUpdate("CREATE INDEX idx_person_name ON person (name)");
        }

        // Populate city table
        try (PreparedStatement stmt =
                 conn.prepareStatement("INSERT INTO city (id, name) VALUES (?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "Forest Hill");
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Denver");
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "St. Petersburg");
            stmt.executeUpdate();
        }

        // Populate person table
        try (PreparedStatement stmt =
                 conn.prepareStatement("INSERT INTO person (id, name, city_id) values (?, ?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "John Doe");
            stmt.setLong(3, 3L);
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Jane Roe");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "Mary Major");
            stmt.setLong(3, 1L);
            stmt.executeUpdate();

            stmt.setLong(1, 4L);
            stmt.setString(2, "Richard Miles");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();
        }

        // Get data
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs =
                     stmt.executeQuery("SELECT p.name, c.name " +
                         " FROM person p, city c " +
                         " WHERE p.city_id = c.id")) {

                System.out.println("Query results:");

                while (rs.next())
                    System.out.println(">>>    " +
                        rs.getString(1) +
                        ", " +
                        rs.getString(2));
            }
        }
    }
}
