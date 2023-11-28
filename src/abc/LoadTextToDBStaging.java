package abc;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LoadTextToDBStaging {
    public static void main(String[] args) {
    	String dataNow = DateNow.getDate();
        String excelFilePath = dataNow + ".xlsx";

        try (FileInputStream fileInputStream = new FileInputStream(excelFilePath);
             Workbook workbook = new XSSFWorkbook(fileInputStream)) {

            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3307/staging", "root", "");

            String sql = "INSERT INTO price_gold (area, date, type, buy, sell) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                // Lặp qua từng sheet trong workbook (nếu có nhiều sheet)
                for (Sheet sheet : workbook) {
                    // Lặp qua từng dòng trong sheet, bắt đầu từ dòng thứ 1 (index 0 là tiêu đề)
                    for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                        Row row = sheet.getRow(rowIndex);

                        // Đọc dữ liệu từ từng ô trong dòng
                        String area = row.getCell(0).getStringCellValue();
                        String date = row.getCell(1).getStringCellValue();
                        String type = row.getCell(2).getStringCellValue();
                        String buy 	= row.getCell(3).getStringCellValue();
                        String sell = row.getCell(4).getStringCellValue();

                        // Đặt giá trị vào PreparedStatement
                        preparedStatement.setString(1, area);
                        preparedStatement.setString(2, date);
                        preparedStatement.setString(3, type);
                        preparedStatement.setString(4, buy);
                        preparedStatement.setString(5, sell);

                        // Thực hiện truy vấn SQL chèn dữ liệu vào cơ sở dữ liệu
                        preparedStatement.executeUpdate();
                    }
                }

                System.out.println("Dữ liệu đã được chèn vào cơ sở dữ liệu.");
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
