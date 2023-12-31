package data_warehouse;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LoadTextToDBStaging {
    public static void load(){
        LocalDateTime currentDateTime = LocalDateTime.now();
        boolean fileFound = false;
        /**
         *  2. Searching file excel to load text
         */
        while (!fileFound) {
            // lấy thời gian hiện tại trừ đi 1 phút
            LocalDateTime previousMinuteDateTime = currentDateTime.minusMinutes(1);
            String datePrevious = previousMinuteDateTime.format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm"));
            String excelFilePath = datePrevious + ".xlsx";

            File file = new File(excelFilePath);
            /**
             *   3. Check file excel exist or not
             *          yes: connect database staging
             *          no: end
             */
            if (file.exists()) {
                fileFound = true;
                System.out.println("Đã tìm thấy file Excel: " + excelFilePath);

                try (FileInputStream fileInputStream = new FileInputStream(excelFilePath);
                     Workbook workbook = new XSSFWorkbook(fileInputStream);
                     Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:" + ConfigInfo.getInstance().getPort() + "/db_staging",
                             ConfigInfo.getInstance().getUsername(), ConfigInfo.getInstance().getPassword())) {

                    /**
                     *   Delete all data in table pricegold to insert new data
                     */
                    String deleteSql = "DELETE FROM price_gold";
                    try (PreparedStatement deleteStatement = connection.prepareStatement(deleteSql)) {
                        deleteStatement.executeUpdate();
                    }

                    /**
                     *  Insert data to table price_gold
                     */
                    // Chuẩn bị câu lệnh chèn dữ liệu
                    String insertSql = "INSERT INTO price_gold (area, date, type, buy, sell) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
                        // Lặp qua từng sheet trong workbook
                        for (Sheet sheet : workbook) {
                            // Lặp qua từng dòng trong sheet, bắt đầu từ dòng thứ 1 (index 0 là tiêu đề)
                            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                                Row row = sheet.getRow(rowIndex);

                                // Đọc dữ liệu từ từng ô trong dòng
                                String area = row.getCell(0).getStringCellValue();
                                String date = row.getCell(1).getStringCellValue();
                                String type = row.getCell(2).getStringCellValue();
                                String buy = row.getCell(3).getStringCellValue();
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
            } else {

                currentDateTime = previousMinuteDateTime;
            }
        }
    }

}
