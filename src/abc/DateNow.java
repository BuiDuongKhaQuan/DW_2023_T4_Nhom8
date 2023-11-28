package abc;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateNow {
	public static String getDate() {
		LocalDateTime currentDateTime = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yyyy");
		return currentDateTime.format(formatter);
	}
}
