package Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class ArrayInsUpd {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("null")
	public void insertDB(ArrayList<String> d) {
		Connection conn = null;
		PreparedStatement ps = null;
		
		try {
			String insert = "insert into eventlog values(?,?)";
			ps = conn.prepareStatement(insert);
			for (int k = 0; k < d.size() - 1; k++) {
				ps.setString(1, d.get(k).toString());
				ps.setString(2, d.get(k).toString());
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}