package abc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConfigInfo {
    private static final String CONFIG_COLUMN_PORT = "db_port";
    private static final String CONFIG_COLUMN_USERNAME = "db_user";
    private static final String CONFIG_COLUMN_PASSWORD = "db_password";
    private static ConfigInfo instance;

    private String port;
    private String username;
    private String password;

    private ConfigInfo(String port, String username, String password) {
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public static ConfigInfo getInstance() {
        if (instance == null) {
            instance = loadConfigInfo();
        }
        return instance;
    }

    private static ConfigInfo loadConfigInfo() {
        String jdbcUrlConfig = "jdbc:mysql://localhost:3307/control";
        String sqlQueryGetConfig = "SELECT * FROM config";
        try {
            Connection connectionConfig = DriverManager.getConnection(jdbcUrlConfig, "root", "");
            try (PreparedStatement preparedStatementGetConfig = connectionConfig.prepareStatement(sqlQueryGetConfig);
                 ResultSet resultSetConfig = preparedStatementGetConfig.executeQuery()) {

                if (resultSetConfig.next()) {
                    String port = resultSetConfig.getString(CONFIG_COLUMN_PORT);
                    String username = resultSetConfig.getString(CONFIG_COLUMN_USERNAME);
                    String password = resultSetConfig.getString(CONFIG_COLUMN_PASSWORD);

                    return new ConfigInfo(port, username, password);
                } else {
                    throw new SQLException("Configuration not found in the config table.");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}