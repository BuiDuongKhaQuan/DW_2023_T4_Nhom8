package data_warehouse;

import java.sql.*;
import java.time.LocalTime;

public class LoadDWToDMART {
    private String urlControl = "jdbc:mysql://localhost:3306/db_control";
    private String urlWarehouse = "jdbc:mysql://localhost:3306/db_warehouse";
    private String urlMart = "jdbc:mysql://localhost:3306/db_mart";
    private String userName = ConfigInfo.getInstance().getUsername();
    private String password = ConfigInfo.getInstance().getPassword();

    private Date getDateLast(Connection conn) throws SQLException {
        String sql = "SELECT MAX(l.id_date) FROM log l WHERE l.status_extract = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, "MART");
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    Date d = resultSet.getDate(1);
                    return (d != null) ? d : Date.valueOf("1111-11-11");
                }
            }
        }
        return Date.valueOf("1111-11-11");
    }
    private void insertDbMart(Connection conn, ResultSet resultSet) throws SQLException {
        System.out.println("insert..." + resultSet.getInt(1) + "-" + resultSet.getInt(5));
        String sql = "INSERT INTO gold (fact_gold_id, type, buy, sell, area_id, area_name, date, time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, resultSet.getInt(1));
            statement.setString(2, resultSet.getString(2));
            statement.setString(3, resultSet.getString(3));
            statement.setString(4, resultSet.getString(4));
            statement.setInt(5, resultSet.getInt(5));
            statement.setString(6, resultSet.getString(6));
            statement.setDate(7, Date.valueOf(resultSet.getString(9)+"-"+resultSet.getString(8)+"-"+resultSet.getString(7)));
            statement.setTime(8, Time.valueOf(resultSet.getString(10)+":"+resultSet.getString(11)+":00"));
            statement.executeUpdate();
        }
    }
    private void insertLog(Connection conn, Date date) throws SQLException {
        System.out.println("insertLog..." + date);
        String sql = "INSERT INTO log (id_config, time_extract, id_date, status_extract) VALUES (?, ?, ?, ?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setObject(2, LocalTime.now());
            statement.setDate(3, date);
            statement.setString(4, "MART");
            statement.executeUpdate();
        }
    }
}
