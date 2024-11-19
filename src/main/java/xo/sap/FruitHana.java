package xo.sap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FruitHana {
    private static final Logger LOG = LoggerFactory.getLogger(FruitHana.class);

    private static final String DB_URL = "jdbc:sap://10.1.125.211:30047";
    private static final String DB_USER = "SAPABAP1";
    private static final String DB_PASSWORD = "E00eld2021";

    public static void main(String[] args) {
        Connection connection = null;

        try {
            // Load the SAP HANA JDBC driver
            Class.forName("com.sap.db.jdbc.Driver");

            // Establish the connection
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println("Connected to SAP HANA database successfully!");

            // Perform a query
            String selectSql = "SELECT ID, NAME, PRICE FROM SAPABAP1.FRUIT2";
            try (PreparedStatement preparedStatement = connection.prepareStatement(selectSql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {

                System.out.println("查询结果：");
                while (resultSet.next()) {
                    int id = resultSet.getInt("ID");
                    String name = resultSet.getString("NAME");
                    double price = resultSet.getDouble("PRICE");
                    System.out.printf("ID: %d, Name: %s, Price: %.2f%n", id, name, price);
                }
            }
        } catch (ClassNotFoundException e) {
            System.err.println("SAP HANA JDBC Driver not found. Add it to your classpath.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Database connection error:");
            e.printStackTrace();
        } finally {
            // Close the connection
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("Connection closed.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
