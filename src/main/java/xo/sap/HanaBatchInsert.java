package xo.sap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;

public class HanaBatchInsert {

    private static final String DB_URL = "jdbc:sap://sap01:30047";
    private static final String DB_USER = "SAPABAP1";
    private static final String DB_PASSWORD = "E00eld2021";
    private static final String FILE_PATH = "valuation.csv";

    private static final String INSERT_SQL = "INSERT INTO SAPABAP1.VALUATION ( " +
        " TDATE, C000010, C000015, C000016, C000300, C000688, C000827, C000852, C000903, C000905, " +
        " C000906, C000919, C000922, C000925, C000932, C000978, C000989, C399001, C399006, C399324, " +
        " C399330, C399393, C399550, C399701, C399702, C399812, C399967, C399975, C399986, C399989, " +
        " C399995, C399997, C707717, C930653, C930697, C930743, C930782, C931009, C931068, C931142, " +
        " C931187, C931357, C950090, CSPSADRP, H30094, H30533, HSCAIT, HSCEI, HSI, HSTECH, " +
        " IXY, NDX, S5INFT, SPG120035, SPHCMSHP, SPX, CNBONE, USBONE, SH000985, STAR" +
        " ) VALUES ( " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            // Load SAP HANA JDBC driver
            Class.forName("com.sap.db.jdbc.Driver");
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false); // Enable batch insert

            try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH));
                 PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL)) {

                String line;
                int batchSize = 500;
                int count = 0;
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");

                    // 1. Set TDATE (Timestamp)
                    preparedStatement.setTimestamp(1,
                            new Timestamp(dateFormat.parse(values[0].trim()).getTime()));

                    // 2. Set FLOAT values (Columns 2 to 54)
                    for (int i = 2; i <= 60; i++) { // 54 float columns (2nd to 55th)
                        String val = values[i - 1].trim(); // Adjust index for 0-based array
                        if (val.isEmpty()) {
                            preparedStatement.setNull(i, Types.FLOAT); // Handle NULL values
                        } else {
                            preparedStatement.setFloat(i, Float.parseFloat(val));
                        }
                    }

                    preparedStatement.addBatch();

                    // Execute batch when batchSize is reached
                    if (++count % batchSize == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                    }
                }

                // Execute remaining batch
                preparedStatement.executeBatch();
                connection.commit();

                System.out.println("CSV Data successfully inserted into SAP HANA!");

            } catch (Exception e) {
                e.printStackTrace();
                connection.rollback();
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
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
