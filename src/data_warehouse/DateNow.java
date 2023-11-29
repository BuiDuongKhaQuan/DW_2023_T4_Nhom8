package data_warehouse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateNow {
	public static String getDate() {
		LocalDateTime currentDateTime = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm");
		return currentDateTime.format(formatter);
	}
}
