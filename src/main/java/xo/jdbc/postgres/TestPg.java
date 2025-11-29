package xo.jdbc.postgres;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestPg {
    private static final String url =
            "jdbc:postgresql://192.168.55.16:5432/postgres?stringtype=unspecified&sslmode=prefer";

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack", "true");
        String user = "manga";
        String password = "manga";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected! " + conn.getMetaData().getDatabaseProductVersion());
        }
    }
}
