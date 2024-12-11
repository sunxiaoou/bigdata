package xo.sap;

import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FruitHana {
    private static final Logger LOG = LoggerFactory.getLogger(FruitHana.class);

    private static final String DB_URL = "jdbc:sap://sap01:30047";
    private static final String DB_USER = "SAPABAP1";
    private static final String DB_PASSWORD = "E00eld2021";
    private static final String regex = "^SAPABAP1.FRUIT.*";
    private static String tableName;
    private static Connection connection = null;

    private static void connect() throws ClassNotFoundException, SQLException {
        // Load the SAP HANA JDBC driver
        Class.forName("com.sap.db.jdbc.Driver");
        connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        LOG.info("Connected to SAP HANA database successfully!");
    }

    private static int put(Triple<Integer, String, Double>[] triples) throws SQLException {
        String insertSql = "INSERT INTO " + tableName + " (ID, NAME, PRICE) VALUES (?, ?, ?)";
        int rowsInserted = 0;
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
            for (Triple<Integer, String, Double> triple : triples) {
                preparedStatement.setInt(1, triple.getFirst());
                preparedStatement.setString(2, triple.getSecond());
                preparedStatement.setDouble(3, triple.getThird());
                rowsInserted += preparedStatement.executeUpdate();
            }
        }
        return rowsInserted;
    }

    private static int put() throws SQLException {
        final Triple<Integer, String, Double>[] triples = new Triple[]{
                new Triple<>(101, "🍉", 800.0),
                new Triple<>(102, "🍓", 150.0),
                new Triple<>(103, "🍎", 120.0),
                new Triple<>(104, "🍋", 200.0),
                new Triple<>(105, "🍊", 115.0),
                new Triple<>(106, "🍌", 110.0)};
        return put(triples);
    }

    private static int add() throws SQLException {
        final Triple<Integer, String, Double>[] triples = new Triple[]{
                new Triple<>(107, "🍐", 115.0)
        };
        return put(triples);
    }

    private static int delete() throws SQLException {
        String deleteSql = "DELETE FROM " + tableName + " WHERE ID = ?";
        int rowsDeleted;
        try (PreparedStatement preparedStatement = connection.prepareStatement(deleteSql)) {
            preparedStatement.setInt(1, 107);
            rowsDeleted = preparedStatement.executeUpdate();
        }
        return rowsDeleted;
    }

    private static int update() throws SQLException {
//        String updateSql = "UPDATE " + tableName + " SET NAME = ?, PRICE = ? WHERE ID = ?";
        String updateSql = "UPDATE " + tableName + " SET PRICE = PRICE * 1.10";
        int rowsUpdated;
        try (PreparedStatement preparedStatement = connection.prepareStatement(updateSql)) {
//            preparedStatement.setString(1, "🍉");
//            preparedStatement.setDouble(2, 600);
//            preparedStatement.setInt(3, 101);
            rowsUpdated = preparedStatement.executeUpdate();
        }
        return rowsUpdated;
    }

    private static int count() throws SQLException {
        String countSql = "SELECT COUNT(*) AS ROW_COUNT FROM " + tableName;
        int rowCount = 0;
        try (PreparedStatement preparedStatement = connection.prepareStatement(countSql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                rowCount = resultSet.getInt("ROW_COUNT");
            }
        }
        return rowCount;
    }

    private static List<Triple<Integer, String, Double>> scan() throws SQLException {
        String selectSql = "SELECT ID, NAME, PRICE FROM " + tableName;
        List<Triple<Integer, String, Double>> list = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(selectSql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                list.add(new Triple<>(
                        resultSet.getInt("ID"),
                        resultSet.getString("NAME"),
                        resultSet.getDouble("PRICE")));
            }
        }
        return list;
    }

    private static void truncate() throws SQLException {
        String truncateSql = "TRUNCATE TABLE " + tableName;
        try (PreparedStatement preparedStatement = connection.prepareStatement(truncateSql)) {
            preparedStatement.executeUpdate();
        }
    }

    private static void createTable() throws SQLException {
        String createSql = "CREATE COLUMN TABLE " +
                tableName +
                " (ID INTEGER CS_INT NOT NULL," +
                " NAME NVARCHAR(10) NOT NULL," +
                " PRICE DOUBLE CS_DOUBLE NOT NULL," +
                " PRIMARY KEY (ID))" +
                " UNLOAD PRIORITY 5 AUTO MERGE ";
        try (PreparedStatement preparedStatement = connection.prepareStatement(createSql)) {
            preparedStatement.executeUpdate();
        }
    }

    private static void dropTable() throws SQLException {
        String dropSql = "DROP TABLE " + tableName;
        try (PreparedStatement preparedStatement = connection.prepareStatement(dropSql)) {
            preparedStatement.executeUpdate();
        }
    }

    private static void run(String action, String table) {
        String defaultName = "SAPABAP1.FRUIT";
        tableName = table == null ? defaultName: table;
        try {
            connect();
            switch (action) {
                case "put":
                    if (tableName.matches(regex)) {
                        System.out.println(put() + " row(s) inserted to " + tableName);
                    } else {
                        LOG.warn("Can only put to \"{}\"", defaultName);
                    }
                    return;
                case "add":
                    if (tableName.matches(regex)) {
                        System.out.println(add() + " row(s) inserted to " + tableName);
                    } else {
                        LOG.warn("Can only put to \"{}\"", defaultName);
                    }
                    return;
                case "delete":
                    if (tableName.matches(regex)) {
                        System.out.println(delete() + " row(s) deleted from " + tableName);
                    } else {
                        LOG.warn("Can only delete from \"{}\"", defaultName);
                    }
                    return;
                case "update":
                    if (tableName.matches(regex)) {
                        System.out.println(update() + " row(s) updated in " + tableName);
                    } else {
                        LOG.warn("Can only update to \"{}\"", defaultName);
                    }
                    return;
                case "count":
                    System.out.println(String.format("%s has %d rows", tableName, count()));
                    return;
                case "scan":
                    if (tableName.matches(regex)) {
                        System.out.println(tableName + " scanned - " + scan());
                    } else {
                        LOG.warn("Can only put to \"{}\"", defaultName);
                    }
                    return;
                case "truncate":
                    truncate();
                    System.out.println("Table " + tableName + " truncated");
                    return;
                case "createTable":
                    createTable();
                    System.out.println("Table " + tableName + " created");
                    return;
                case "dropTable":
                    dropTable();
                    System.out.println("Table " + tableName + " dropped");
                    return;
                default:
                    System.out.println("Unknown action: " + action);
            }
        } catch (ClassNotFoundException | SQLException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length > 1) {
            run(args[0], args[1]);
        } else if (args.length > 0) {
            run(args[0], null);
        } else {
            System.out.println("Usage: hana put|add|delete|update|count|scan|truncate [table]");
            System.out.println("       hana createTable|dropTable [table]");
            System.exit(1);
        }
    }
}
