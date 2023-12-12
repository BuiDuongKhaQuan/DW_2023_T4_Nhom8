package data_warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoadDBStagingToDW {

	public static void main(String[] args) {
		// Retrieve configuration parameters
		String port = ConfigInfo.getInstance().getPort();
		String userName = ConfigInfo.getInstance().getUsername();
		String password = ConfigInfo.getInstance().getPassword();
		String jdbcUrlWarehouse = "jdbc:mysql://localhost:" + port + "/data_warehouse";
		String jdbcUrlStaging = "jdbc:mysql://localhost:" + port + "/staging";

		try ( // Establish connections to control and staging databases using JDBC
				Connection connectionWarehouse = DriverManager.getConnection(jdbcUrlWarehouse, userName, password);
				Connection connectionStaging = DriverManager.getConnection(jdbcUrlStaging, userName, password)) {
			// Execute SQL query to get all data from staging table "price_gold"
			String sqlQueryGetAllStaging = "SELECT * FROM price_gold;";
			try (PreparedStatement preparedStatementStaging = connectionStaging.prepareStatement(sqlQueryGetAllStaging);
					ResultSet resultSet = preparedStatementStaging.executeQuery()) {
				// Loop through the result set
				while (resultSet.next()) {
					// Extract information from each row of result set (province, date, type, buy,
					// sell)
					String province = resultSet.getString("area");
					String date = resultSet.getString("date");
					String type = resultSet.getString("type");
					String buy = resultSet.getString("buy");
					String sell = resultSet.getString("sell");

					// Check if Data Exists in dim_date
					int idDate = getIdIfExists(connectionWarehouse, "dim_date", "day", "month", "year", "hour", "minute",
							date);

					if (idDate == -1) {
						// Insert the date into the dim_date table and retrieve the generated id_date
						String[] dateParts = date.split("/");
						String sqlQueryDimDate = "INSERT INTO dim_date (day, month, year, hour, minute) VALUES (?, ?, ?, ?, ?)";
						try (PreparedStatement preparedStatementDimDate = connectionWarehouse
								.prepareStatement(sqlQueryDimDate, PreparedStatement.RETURN_GENERATED_KEYS)) {
							preparedStatementDimDate.setString(1, dateParts[0]); // day
							preparedStatementDimDate.setString(2, dateParts[1]); // month
							preparedStatementDimDate.setString(3, dateParts[2]); // year
							preparedStatementDimDate.setString(4, dateParts[3]); // hour
							preparedStatementDimDate.setString(5, dateParts[4]); // minute
							preparedStatementDimDate.executeUpdate();

							// Retrieve the generated id_date
							try (ResultSet generatedKeys = preparedStatementDimDate.getGeneratedKeys()) {
								if (generatedKeys.next()) {
									idDate = generatedKeys.getInt(1);
								} else {
									throw new SQLException("Failed to retrieve id_date from dim_date.");
								}
							}
						}
					}

					// Check if Data Exists in dim_area
					int idProvince = getIdIfExists(connectionWarehouse, "dim_area", "name", province);
					if (idProvince == -1) {
						// insert the province into the dim_province table and retrieve the generated
						// id_area
						String sqlQueryDimProvince = "INSERT INTO dim_area (name) VALUES (?)";
						try (PreparedStatement preparedStatementDimProvince = connectionWarehouse
								.prepareStatement(sqlQueryDimProvince, PreparedStatement.RETURN_GENERATED_KEYS)) {
							preparedStatementDimProvince.setString(1, province);
							preparedStatementDimProvince.executeUpdate();

							// Retrieve the generated id_area
							try (ResultSet generatedKeysProvince = preparedStatementDimProvince.getGeneratedKeys()) {
								if (generatedKeysProvince.next()) {
									idProvince = generatedKeysProvince.getInt(1);
								} else {
									throw new SQLException("Failed to retrieve dim_area from dim_province.");
								}
							}
						}
					}

					// Check if Data Exists in dim_gold_type
					int idGoldType = getIdIfExists(connectionWarehouse, "dim_gold_type", "type", type);
					if (idGoldType == -1) {
						// insert the gold type into the dim_gold_type table and retrieve the generated
						// id_gold_type
						String sqlQueryDimGoldType = "INSERT INTO dim_gold_type (type) VALUES (?)";
						try (PreparedStatement preparedStatementDimGoldType = connectionWarehouse
								.prepareStatement(sqlQueryDimGoldType, PreparedStatement.RETURN_GENERATED_KEYS)) {
							preparedStatementDimGoldType.setString(1, type);
							preparedStatementDimGoldType.executeUpdate();

							// Retrieve the generated id_gold_type
							try (ResultSet generatedKeysGoldType = preparedStatementDimGoldType.getGeneratedKeys()) {
								if (generatedKeysGoldType.next()) {
									idGoldType = generatedKeysGoldType.getInt(1);
								} else {
									throw new SQLException("Failed to retrieve id_gold_type from dim_gold_type.");
								}
							}
						}
					}

					// Insert data into fact_gold_price table
					String sqlQueryFactGoldPrice = "INSERT INTO fact_gold_price (id_date, id_area, id_gold_type, buy, sell) VALUES (?, ?, ?, ?, ?)";
					try (PreparedStatement preparedStatementFactGoldPrice = connectionWarehouse
							.prepareStatement(sqlQueryFactGoldPrice)) {
						preparedStatementFactGoldPrice.setInt(1, idDate);
						preparedStatementFactGoldPrice.setInt(2, idProvince);
						preparedStatementFactGoldPrice.setInt(3, idGoldType);
						preparedStatementFactGoldPrice.setString(4, buy);
						preparedStatementFactGoldPrice.setString(5, sell);
						preparedStatementFactGoldPrice.executeUpdate();
					}
					// Loop to the next row in the result set
					// Continue processing until all rows are processed
				}
			}
			System.out.println("Tiến trình hoàn tất!");
			// Close connections
			connectionWarehouse.close();
			connectionStaging.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Function to get the ID if the record already exists
	private static int getIdIfExists(Connection connection, String tableName, String columnName, String value)
			throws SQLException {
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

	// Overloaded function for multiple columns including hour and minute
	private static int getIdIfExists(Connection connection, String tableName, String columnName1, String columnName2,
			String columnName3, String columnName4, String columnName5, String value) throws SQLException {
		String sqlQueryGetId = "SELECT id FROM " + tableName + " WHERE " + columnName1 + " = ? AND " + columnName2
				+ " = ? AND " + columnName3 + " = ? AND " + columnName4 + " = ? AND " + columnName5 + " = ?";
		String[] dateParts = value.split("/");
		try (PreparedStatement preparedStatementGetId = connection.prepareStatement(sqlQueryGetId)) {
			preparedStatementGetId.setString(1, dateParts[0]); // day
			preparedStatementGetId.setString(2, dateParts[1]); // month
			preparedStatementGetId.setString(3, dateParts[2]); // year
			preparedStatementGetId.setString(4, dateParts[3]); // hour
			preparedStatementGetId.setString(5, dateParts[4]); // minute
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
