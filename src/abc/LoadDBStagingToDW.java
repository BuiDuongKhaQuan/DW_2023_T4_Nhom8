package abc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadDBStagingToDW {
	public static void main(String[] args) {
        String jdbcUrlControl = "jdbc:mysql://localhost:3307/data_warehouse";
        String jdbcUrlStaging = "jdbc:mysql://localhost:3307/staging";
        String username = "root";
        String password = "";

        try (
            Connection connectionControl = DriverManager.getConnection(jdbcUrlControl, username, password);
            Connection connectionStaging = DriverManager.getConnection(jdbcUrlStaging, username, password)
        ) {
            // Câu lệnh SQL để lấy dữ liệu từ bảng price_gold trong staging
            String sqlQueryGetAllStaging = "SELECT * FROM price_gold;";
            try (PreparedStatement preparedStatementStaging = connectionStaging.prepareStatement(sqlQueryGetAllStaging);
                ResultSet resultSet = preparedStatementStaging.executeQuery()) {

                while (resultSet.next()) {
                    // Lấy thông tin từ bảng price_gold
                    String province = resultSet.getString("area");
                    String date = resultSet.getString("date");
                    String type = resultSet.getString("type");
                    String buy = resultSet.getString("buy");
                    String sell = resultSet.getString("sell");

                    // Check if data already exists in dim_date
                    int idDate = getIdIfExists(connectionControl, "dim_date", "day", "month", "year", date);

                    if (idDate == -1) {
                        // Thêm dữ liệu vào bảng dim_date
                        String[] dateParts = date.split("/");
                        String sqlQueryDimDate = "INSERT INTO dim_date (day, month, year) VALUES (?, ?, ?)";
                        try (PreparedStatement preparedStatementDimDate = connectionControl.prepareStatement(sqlQueryDimDate, PreparedStatement.RETURN_GENERATED_KEYS)) {
                            preparedStatementDimDate.setString(1, dateParts[0]); // day
                            preparedStatementDimDate.setString(2, dateParts[1]); // month
                            preparedStatementDimDate.setString(3, dateParts[2]); // year
                            preparedStatementDimDate.executeUpdate();

                            // Lấy id_date từ dim_date
                            try (ResultSet generatedKeys = preparedStatementDimDate.getGeneratedKeys()) {
                                if (generatedKeys.next()) {
                                    idDate = generatedKeys.getInt(1);
                                } else {
                                    throw new SQLException("Failed to retrieve id_date from dim_date.");
                                }
                            }
                        }
                    }

                    // Similar checks for dim_province and dim_gold_type
                    int idProvince = getIdIfExists(connectionControl, "dim_province", "name", province);
                    if (idProvince == -1) {
                        // Thêm dữ liệu vào bảng dim_province
                        String sqlQueryDimProvince = "INSERT INTO dim_province (name) VALUES (?)";
                        try (PreparedStatement preparedStatementDimProvince = connectionControl.prepareStatement(sqlQueryDimProvince, PreparedStatement.RETURN_GENERATED_KEYS)) {
                            preparedStatementDimProvince.setString(1, province);
                            preparedStatementDimProvince.executeUpdate();

                            // Lấy id_province từ dim_province
                            try (ResultSet generatedKeysProvince = preparedStatementDimProvince.getGeneratedKeys()) {
                                if (generatedKeysProvince.next()) {
                                    idProvince = generatedKeysProvince.getInt(1);
                                } else {
                                    throw new SQLException("Failed to retrieve id_province from dim_province.");
                                }
                            }
                        }
                    }

                    int idGoldType = getIdIfExists(connectionControl, "dim_gold_type", "type", type);
                    if (idGoldType == -1) {
                        // Thêm dữ liệu vào bảng dim_gold_type
                        String sqlQueryDimGoldType = "INSERT INTO dim_gold_type (type) VALUES (?)";
                        try (PreparedStatement preparedStatementDimGoldType = connectionControl.prepareStatement(sqlQueryDimGoldType, PreparedStatement.RETURN_GENERATED_KEYS)) {
                            preparedStatementDimGoldType.setString(1, type);
                            preparedStatementDimGoldType.executeUpdate();

                            // Lấy id_gold_type từ dim_gold_type
                            try (ResultSet generatedKeysGoldType = preparedStatementDimGoldType.getGeneratedKeys()) {
                                if (generatedKeysGoldType.next()) {
                                    idGoldType = generatedKeysGoldType.getInt(1);
                                } else {
                                    throw new SQLException("Failed to retrieve id_gold_type from dim_gold_type.");
                                }
                            }
                        }
                    }

                    // Thêm dữ liệu vào bảng fact_gold_price
                    String sqlQueryFactGoldPrice = "INSERT INTO fact_gold_price (id_date, id_province, id_gold_type, buy, sell) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement preparedStatementFactGoldPrice = connectionControl.prepareStatement(sqlQueryFactGoldPrice)) {
                        preparedStatementFactGoldPrice.setInt(1, idDate);
                        preparedStatementFactGoldPrice.setInt(2, idProvince);
                        preparedStatementFactGoldPrice.setInt(3, idGoldType);
                        preparedStatementFactGoldPrice.setString(4, buy);
                        preparedStatementFactGoldPrice.setString(5, sell);
                        preparedStatementFactGoldPrice.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Function to get the ID if the record already exists
    private static int getIdIfExists(Connection connection, String tableName, String columnName, String value) throws SQLException {
        String sqlQueryGetId = "SELECT id FROM " + tableName + " WHERE " + columnName + " = ?";
        try (PreparedStatement preparedStatementGetId = connection.prepareStatement(sqlQueryGetId)) {
            preparedStatementGetId.setString(1, value);
            try (ResultSet resultSetId = preparedStatementGetId.executeQuery()) {
                if (resultSetId.next()) {
                    return resultSetId.getInt("id");
                } else {
                    return -1; // Return -1 if the record does not exist
                }
            }
        }
    }

    // Overloaded function for multiple columns
    private static int getIdIfExists(Connection connection, String tableName, String columnName1, String columnName2, String columnName3, String value) throws SQLException {
        String sqlQueryGetId = "SELECT id FROM " + tableName + " WHERE " + columnName1 + " = ? AND " + columnName2 + " = ? AND " + columnName3 + " = ?";
        String[] dateParts = value.split("/");
        try (PreparedStatement preparedStatementGetId = connection.prepareStatement(sqlQueryGetId)) {
            preparedStatementGetId.setString(1, dateParts[0]); // day
            preparedStatementGetId.setString(2, dateParts[1]); // month
            preparedStatementGetId.setString(3, dateParts[2]); // year
            try (ResultSet resultSetId = preparedStatementGetId.executeQuery()) {
                if (resultSetId.next()) {
                    return resultSetId.getInt("id");
                } else {
                    return -1; // Return -1 if the record does not exist
                }
            }
        }
    }
}
