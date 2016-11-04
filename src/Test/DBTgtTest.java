package Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DBTgtTest {

	public static void main(String[] args) {
		DBSelect();
		
		DBInsert();
		
		DBUpdate();
		
		DBSelect();
	}
	
	public static void DBSelect() {

		try{
			String myDriver = "org.mariadb.jdbc.Driver";
		    String myUrl = "jdbc:mariadb://10.1.11.13:3306/monitor";
		    Class.forName(myDriver);
		    Connection conn = DriverManager.getConnection(myUrl, "dba", "ICSys07!@");
		    
		    String query = 
		    		    "SELECT            "
					  + " SEQ,             "
					  + " HOSTNAME,        "
					  + " DBNAME,          "
					  + " REGDATE,         "
					  + " BACKUPSTATUS,    "
					  + " STORAGESIZE,     "
					  + " DBTYPE,          "
					  + " LOCATION,        "
					  + " CRONTIME         "
					  + "FROM BkpmonMaster ";
		    PreparedStatement pstmt = null;
		    pstmt = conn.prepareStatement(query);
		    
		    ResultSet rs = pstmt.executeQuery(query);
		    
			while (rs.next()) {
				 System.out.format("%s, %s, %s, %s, %s, %s, %s, %s, %s\n", 
						 rs.getInt("SEQ"),
						 rs.getString("HOSTNAME"), 
						 rs.getString("DBNAME"),
						 rs.getDate("REGDATE"), 
						 rs.getString("BACKUPSTATUS"),
						 rs.getInt("STORAGESIZE"), 
						 rs.getString("DBTYPE"),
						 rs.getString("LOCATION"), 
						 rs.getInt("CRONTIME"));

				//System.out.println("DB Sequence ==> " + rs.getInt("SEQ"));
			}
			
			pstmt.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void DBInsert() {

		try{
			String myDriver = "org.mariadb.jdbc.Driver";
		    String myUrl = "jdbc:mariadb://10.1.11.13:3306/monitor";
		    Class.forName(myDriver);
		    Connection conn = DriverManager.getConnection(myUrl, "dba", "ICSys07!@");
		    
		    String query = 
		    		  "INSERT INTO BkpmonMaster ( "
		    		+ " HOSTNAME,                 "
		    		+ " DBNAME,                   "
		    		+ " BACKUPSTATUS,             "
		    		+ " DBTYPE,                   "
		    		+ " LOCATION,                 "
		    		+ " CRONTIME)                 "
		    		+ "VALUES (                   "
		    		+ " ?,                        "
		    		+ " ?,                        "
		    		+ " ?,                        "
		    		+ " ?,                        "
		    		+ " ?,                        "
		    		+ " ?)                        ";
		    PreparedStatement pstmt = null;
		    pstmt = conn.prepareStatement(query);
		    
		    pstmt.setString(1, "thostname");
		    pstmt.setString(2, "tdbname");
		    pstmt.setString(3, "S");
		    pstmt.setString(4, "ORACLE");
		    pstmt.setString(5, "tktidc");
		    pstmt.setInt(6, 1);
		    
		    int iInsCnt = pstmt.executeUpdate();
		    
			if (iInsCnt > 0) {
				System.out.println("Insert Success");
			} else {
				System.out.println("Insert Fail");
			}
			
			pstmt.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void DBUpdate() {

		try{
			String myDriver = "org.mariadb.jdbc.Driver";
		    String myUrl = "jdbc:mariadb://10.1.11.13:3306/monitor";
		    Class.forName(myDriver);
		    Connection conn = DriverManager.getConnection(myUrl, "dba", "ICSys07!@");
		    
		    String query = 
		    		  "UPDATE BkpmonMaster    "
		    		+ " SET BACKUPSTATUS = ?  "
		    		+ "WHERE HOSTNAME = ?     "
		    		+ "AND DBNAME = ?         ";
		    PreparedStatement pstmt = null;
		    pstmt = conn.prepareStatement(query);
		    
		    pstmt.setString(1, "E");
		    pstmt.setString(2, "thostname");
		    pstmt.setString(3, "tdbname");
		    
		    int iUpdCnt = pstmt.executeUpdate();

			if (iUpdCnt > 0) {
				System.out.println("Update Success");
			} else {
				System.out.println("Update Fail");
			}
			
			pstmt.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}