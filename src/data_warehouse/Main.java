package data_warehouse;

import java.sql.SQLException;

public class Main {
	public static void main(String[] args) throws SQLException {
//        ExtractData.extract();
//        LoadTextToDBStaging.load();
		LoadDBStagingToDW.loadDW();
	}
}
