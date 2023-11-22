import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ExtractData {
	
    public static void main(String[] args) {
    	
    	String jdbcUrl = "jdbc:mysql://localhost:3306/control_phuc";
        String username = "root";
        String password = "";
        
        
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
//            System.out.println("Ghi lại status SE");

            // Câu lệnh SQL để thêm dữ liệu vào bảng log
            String sqlQuery = "INSERT INTO log ( time_extract, id_date, status_extract) VALUES ( TIME(NOW()), CURDATE(), 'SE');";

            // Sử dụng PreparedStatement để thực hiện câu lệnh SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);

            // Sử dụng executeUpdate() thay vì executeQuery() để thực hiện INSERT
            int rowsAffected = preparedStatement.executeUpdate();

            // Đóng PreparedStatement
            preparedStatement.close();

            // Đóng kết nối sau khi thêm dữ liệu.
            connection.close();

            System.out.println("status SE đã được thêm vào bảng log");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Lấy dữ liệu cho Hồ Chí Minh và Cần Thơ
        List<String[]> hoChiMinhData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=00", "Hồ Chí Minh");
        List<String[]> canThoData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=07", "Cần Thơ");
        List<String[]> HaNoiData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=11", "Hà Nội");
        List<String[]> DaNangData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=13", "Đà Nẵng");
        List<String[]> TayNguyenData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=14", "Tây Nguyên");
        List<String[]> DNBData = fetchData("https://www.pnj.com.vn/blog/gia-vang/?zone=21", "Đông Nam Bộ");
        // Ghi dữ liệu vào cùng một file Excel
        writeToExcel(hoChiMinhData, canThoData,HaNoiData,DaNangData,TayNguyenData,DNBData, "output_combined.xlsx");

        System.out.println("Dữ liệu đã được ghi vào file Excel.");
        //ghi lại status vô bảng log
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
//            System.out.println("Kiểm tra status trong bảng log");

            // Câu lệnh SQL để kiểm tra xem có bất kỳ bản ghi nào trong ngày hôm đó có status_extract khác 'FE' không
            String checkQuery = "SELECT COUNT(*) AS count FROM log WHERE DATE(id_date) = CURDATE() AND status_extract = 'FE'";
            PreparedStatement checkStatement = connection.prepareStatement(checkQuery);
            ResultSet resultSet = checkStatement.executeQuery();

            // Kiểm tra kết quả của truy vấn
            if (resultSet.next()) {
                int count = resultSet.getInt("count");
                if (count == 0) {
                    // Không có bản ghi nào trong ngày hôm đó có status khác 'FE', bạn có thể chèn dữ liệu
                    String insertQuery = "INSERT INTO log ( time_extract, id_date, status_extract) VALUES ( TIME(NOW()), CURDATE(), 'CE');";
                    PreparedStatement insertStatement = connection.prepareStatement(insertQuery);
                    int rowsAffected = insertStatement.executeUpdate();
                    insertStatement.close();
                    System.out.println("status CE đã được thêm vào bảng log");
                } else {
                    System.out.println("Do trạng thái extract là FE nên không insert CE vào status");
                }
            }

            // Đóng ResultSet, PreparedStatement và kết nối
            resultSet.close();
            checkStatement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        
       
    }
    
    

    private static List<String[]> fetchData(String url, String cityName) {
        List<String[]> data = new ArrayList<>();

        try {
            // Kết nối với trang web
            Document doc = Jsoup.connect(url).get();

            // Lấy ngày cập nhật
            String ngayCapNhatStr = doc.select("#time-now").text();
            LocalDateTime ngayCapNhat = parseNgayCapNhat(ngayCapNhatStr);

            // Lấy danh sách các hàng trong bảng giá vàng
            Elements rows = doc.select("#content-price tr");

            // Duyệt qua từng hàng trong bảng
            for (Element row : rows) {
                // Lấy dữ liệu từ các ô trong hàng
                Elements columns = row.select("td");
                String loaiVang = columns.get(0).text();
                String giaMua = columns.get(1).select("span").text();
                String giaBan = columns.get(2).select("span").text();

                // Thêm dữ liệu vào danh sách
                String[] rowData = { cityName, ngayCapNhat.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")), loaiVang, giaMua, giaBan };
                data.add(rowData);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    private static LocalDateTime parseNgayCapNhat(String ngayCapNhatStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
        return LocalDateTime.parse(ngayCapNhatStr, formatter);
    }

    private static void writeToExcel(List<String[]> hoChiMinhData, List<String[]> canThoData,List<String[]> HaNoiData,List<String[]> DaNangData,List<String[]> TayNguyenData,List<String[]> DNBData, String filePath) {
        try (Workbook workbook = new XSSFWorkbook()) {
            // Tạo một sheet trong workbook
            Sheet sheet = workbook.createSheet("GiaVang");

            // Ghi dữ liệu cho Hồ Chí Minh
            writeDataToSheet(hoChiMinhData, sheet);

            // Ghi dữ liệu cho Cần Thơ
            writeDataToSheet(canThoData, sheet);
            
            // Ghi dữ liệu cho Hà Nội
            writeDataToSheet(HaNoiData, sheet);
            
            // Ghi dữ liệu cho Đà Nẵng
            writeDataToSheet(DaNangData, sheet);
            
         // Ghi dữ liệu cho Tây Nguyên
            writeDataToSheet(TayNguyenData, sheet);
            
            // Ghi dữ liệu cho Đông Nam Bộ
            writeDataToSheet(DNBData, sheet);

            // Ghi workbook vào file Excel
            try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
                workbook.write(outputStream);
            }

        } catch (IOException e) {
        	 try {
        		 String jdbcUrl = "jdbc:mysql://localhost:3306/control_phuc";
        	        String username = "root";
        	        String password = "";
                 Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
//                 System.out.println("Ghi lại status SE");

                 // Câu lệnh SQL để thêm dữ liệu vào bảng log
                 String sqlQuery = "INSERT INTO log ( time_extract, id_date, status_extract) VALUES ( TIME(NOW()), CURDATE(), 'FE');";

                 // Sử dụng PreparedStatement để thực hiện câu lệnh SQL
                 PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);

                 // Sử dụng executeUpdate() thay vì executeQuery() để thực hiện INSERT
                 int rowsAffected = preparedStatement.executeUpdate();

                 // Đóng PreparedStatement
                 preparedStatement.close();

                 // Đóng kết nối sau khi thêm dữ liệu.
                 connection.close();

                 System.out.println("status FE đã được thêm vào bảng log do không insert dc dữ liệu");
             } catch (SQLException ex) {
                 ex.printStackTrace();
             }
           
        }
    }

    private static void writeDataToSheet(List<String[]> data, Sheet sheet) {
        int rowNum = sheet.getLastRowNum() + 1; // Lấy số dòng hiện tại và tăng thêm 1
        for (String[] rowData : data) {
            Row row = sheet.createRow(rowNum++);
            int colNum = 0;
            for (String value : rowData) {
                Cell cell = row.createCell(colNum++);
                cell.setCellValue(value);
            }
        }
        
    }
    
    
    
}
    
  

    
    
    
    
    
    
    
    
    

