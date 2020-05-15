package io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLSource {
    private static String url;
    private static String user;
    private static String password;

    private SQLSource() {
    }

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (java.lang.ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        url = System.getProperty("database.url", "jdbc:postgresql://18.221.92.236:5432/mydb");
        user = System.getProperty("database.username", "mydb");
        password = System.getProperty("database.password", "mydb");
        return DriverManager.getConnection(url, user, password);
    }
}
