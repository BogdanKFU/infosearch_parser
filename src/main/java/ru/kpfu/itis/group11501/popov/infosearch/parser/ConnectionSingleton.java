package ru.kpfu.itis.group11501.popov.infosearch.parser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Bogdan Popov on 18.02.2019.
 */

public class ConnectionSingleton {
    private static Connection connection = null;
    private final static String url = "";
    private final static String username = "";
    private final static String password = "";
    private final static String driver = "org.postgresql.Driver";

    public static Connection getConnection() throws SQLException {
        if (connection == null) {
            try {
                Class.forName(driver);
                connection = DriverManager.getConnection(url, username, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
