package data_warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogWriter {

    private String jdbcUrl;
    private String username;
    private String password;

    public LogWriter(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void writeToLog(String status) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // Câu lệnh SQL để thêm dữ liệu vào bảng log
            String sqlQuery = "INSERT INTO log (time_extract, id_date, status_extract) VALUES (TIME(NOW()), CURDATE(), ?);";

            // Sử dụng PreparedStatement để thực hiện câu lệnh SQL
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                // Đặt giá trị cho tham số trong câu lệnh SQL
                preparedStatement.setString(1, status);

                // Sử dụng executeUpdate() thay vì executeQuery() để thực hiện INSERT
                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println(rowsAffected + " row(s) affected in log table.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
