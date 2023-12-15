package org.example;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

public class LoadDWToDM {
    // Các URL kết nối đến cơ sở dữ liệu
    private static String userName = ConfigInfo.getInstance().getUsername();
    private static String port = ConfigInfo.getInstance().getPort();
    private static String password = ConfigInfo.getInstance().getPassword();

    private static String urlControl = "jdbc:mysql://localhost:"+port+"/db_control";
    private static String urlWarehouse = "jdbc:mysql://localhost:"+port+"/db_warehouse";
    private static String urlMart = "jdbc:mysql://localhost:"+port+"/db_mart";
    // Tên người dùng và mật khẩu để kết nối đến cơ sở dữ liệu


    /**
     * Hàm kết nối đến cơ sở dữ liệu
     *
     * @param url URL của cơ sở dữ liệu cần kết nối
     * @return Đối tượng Connection kết nối đến cơ sở dữ liệu
     * @throws SQLException Nếu có lỗi xảy ra trong quá trình kết nối
     */
    private Connection connectDb(String url) throws SQLException {
        return DriverManager.getConnection(url, userName, password);
    }
    /**
     * Hàm tải dữ liệu từ kho dữ liệu (Data Warehouse) vào cửa hàng dữ liệu (Data Mart)
     *
     * @param date1 Ngày bắt đầu
     * @param date2 Ngày kết thúc
     */
    public void loadData(Date date1, Date date2) {
        try (Connection connectionControl = connectDb(urlControl);
             Connection connectionWarehouse = connectDb(urlWarehouse);
             Connection connectionMart = connectDb(urlMart)) {
            if (date1 != null && date2 != null) {
                deleteGold(connectionMart, date1, date2);
                loading(connectionWarehouse, connectionMart, connectionControl, date1, date2);
            } else {
                Date date = getMaxDate(connectionControl);
                System.out.println("DateLast = " + date);
                String sql = "SELECT DISTINCT CONCAT(d.year, '-', d.month, '-', d.day) " +
                        "FROM dim_date d WHERE CONCAT(d.year, '-', d.month, '-', d.day) > ?";
                try (PreparedStatement statement = connectionWarehouse.prepareStatement(sql)) {
                    statement.setDate(1, date);
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            deleteGold(connectionMart, rs.getDate(1), rs.getDate(1));
                            loading(connectionWarehouse, connectionMart, connectionControl,
                                    rs.getDate(1), rs.getDate(1));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Xóa dữ liệu vàng từ cơ sở dữ liệu trong một khoảng thời gian cụ thể.
     *
     * @param conn  Kết nối đến cơ sở dữ liệu
     * @param date1 Ngày bắt đầu của khoảng thời gian cần xóa
     * @param date2 Ngày kết thúc của khoảng thời gian cần xóa
     * @throws SQLException Ném ra khi có lỗi xảy ra trong quá trình thực hiện truy vấn SQL
     */
    private void deleteGold(Connection conn, Date date1, Date date2) throws SQLException {
        System.out.println("Delete gold " + date1 + " -> " + date2);
        String sql = "DELETE from Gold where `date` between ? and ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setDate(1, date1);
            statement.setDate(2, date2);
            statement.executeUpdate();
        }
    }
    /**
     * Hàm lấy ngày mới nhất đã được insert dữ liệu từ bảng log
     *
     * @param conn Đối tượng Connection kết nối đến cơ sở dữ liệu
     * @return Ngày cuối cùng từ bảng log
     * @throws SQLException Nếu có lỗi xảy ra trong quá trình truy vấn dữ liệu
     */
    private Date getMaxDate(Connection conn) throws SQLException {
        String sql = "SELECT MAX(l.id_date) FROM log l WHERE l.status_extract = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, "MART");
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    Date date = resultSet.getDate(1);
                    return date != null ? date : Date.valueOf("1111-11-11");
                }
            }
        }
        return Date.valueOf("1111-11-11");
    }
    /**
     * Hàm tải dữ liệu lên từ kho dữ liệu (Data Warehouse)
     *
     * @param connWH      Đối tượng Connection kết nối đến kho dữ liệu
     * @param connMART    Đối tượng Connection kết nối đến cửa hàng dữ liệu
     * @param connCONTROL Đối tượng Connection kết nối đến cơ sở dữ liệu điều khiển
     * @param date1       Ngày bắt đầu
     * @param date2       Ngày kết thúc
     * @throws SQLException Nếu có lỗi xảy ra trong quá trình truy vấn dữ liệu
     */
    private void loading(Connection connWH, Connection connMART, Connection connCONTROL, Date date1, Date date2) throws SQLException {
        String sql = "SELECT f.id AS fact_gold_id, t.type, f.buy, f.sell, f.id_area AS area_id, " +
                "a.name AS area_name, d.day, d.month, d.year, d.hour, d.minute " +
                "FROM fact_gold_price f " +
                "INNER JOIN dim_date d ON f.id_date = d.id " +
                "INNER JOIN dim_gold_type t ON t.id = f.id_gold_type " +
                "INNER JOIN dim_area a ON a.id = f.id_area " +
                "WHERE CONCAT(d.year, '-', d.month, '-', d.day) between ? and ?";
        try (PreparedStatement statement = connWH.prepareStatement(sql)) {
            statement.setDate(1, date1);
            statement.setDate(2, date2);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    insertDbMart(connMART, resultSet);
                }
            }
        }
        insertLog(connCONTROL, date2);
    }
    /**
     * @param conn      Đối tượng Connection kết nối đến data_mart
     * @param resultSet dữ liệu của 1 gold
     * @throws SQLException Nếu có lỗi xảy ra trong quá trình truy vấn dữ liệu
     */
    private void insertDbMart(Connection conn, ResultSet resultSet) throws SQLException {
        System.out.println("insert...fact_id = " + resultSet.getInt(1));
        String sql = "INSERT INTO gold (fact_gold_id, type, buy, sell, area_id, area_name, date, time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, resultSet.getInt(1));
            statement.setString(2, resultSet.getString(2));
            statement.setString(3, resultSet.getString(3));
            statement.setString(4, resultSet.getString(4));
            statement.setInt(5, resultSet.getInt(5));
            statement.setString(6, resultSet.getString(6));
            statement.setDate(7, Date.valueOf(resultSet.getString(9) + "-" + resultSet.getString(8) + "-" + resultSet.getString(7)));
            statement.setTime(8, Time.valueOf(resultSet.getString(10) + ":" + resultSet.getString(11) + ":00"));
            statement.executeUpdate();
        }
    }
    /**
     * @param conn Đối tượng Connection kết nối đến da_control
     * @param date ngày được load vào mart
     * @throws SQLException Nếu có lỗi xảy ra trong quá trình truy vấn dữ liệu
     */
    private void insertLog(Connection conn, Date date) throws SQLException {
        String sql = "INSERT INTO log (id_config, time_extract, id_date, status_extract) VALUES (?, ?, ?, ?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setObject(2, LocalTime.now());
            statement.setDate(3, date);
            statement.setString(4, "MART");
            statement.executeUpdate();
        }
    }
    private static boolean chkDate(String d) {
        try {
            LocalDate.parse(d);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
    public static void main(String[] args) {
        if (args.length == 1) {
            if(chkDate(args[0])) {
                new LoadDWToDM().loadData(Date.valueOf(args[0]), Date.valueOf(args[0]));
            } else {
                System.out.println("Sai định dạng ngày tháng");
            }
        } else if (args.length == 2) {
            if(chkDate(args[0])&&chkDate(args[1])) {
                LocalDate date1 = LocalDate.parse(args[0]);
                LocalDate date2 = LocalDate.parse(args[1]);

                if (date2.isBefore(date1)) {
                    System.out.println("Ngày 2 phải sau ngày 1");
                } else {
                    new LoadDWToDM().loadData(Date.valueOf(args[0]), Date.valueOf(args[1]));
                }
            } else {
                System.out.println("Sai định dạng ngày tháng");
            }
        } else if (args.length == 0) {
            new LoadDWToDM().loadData(null, null);
        }
    }
}
