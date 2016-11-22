import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

public class BkpMonAgent {

	static boolean chkctrlfile = false, chkarcfile = false;
	
	static String loglevel, dbtype, bkpmonrsltfile;
	static String srcdbclass, srcdburl, srcuser, srcpasswd;
	static String tgtdbclass, tgtdburl, tgtuser, tgtpasswd;
	static String hostnm, agenthomedir,	oraclebkpdir, oraclearcdir, oracledbnm, 
				  location, prcscmdstr, diskcmdstr, bkpcmdstr, bkpstatus, todaydate;
	static String dfrsltstr = "", bkprsltstr = "";
	static int crontime;
	static HashMap<String, String> hashbkpfile = new HashMap<String, String>();
	static ArrayList<String> arrctrllist = new ArrayList<String>();
	
	static ArrayList<String> arrdisklog = new ArrayList<String>();
	static ArrayList<String> arrbkplog = new ArrayList<String>();
	
	public static void main(String[] args) {
	
		try {
			Date myDate = new Date();
			SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMdd");
			myDate.setTime(System.currentTimeMillis());
			todaydate = dayTime.format(myDate);
			
			LoadAgentInfo(args[0]); // Load Info
			
			if(loglevel.equals("DEBUG")) {
				System.out.println("=============================================== Agent Usage =================================================================");
				System.out.println("===  cd /ORACLE/Monitoring/Agent                                                                                          ===");
				System.out.println("===  Compile :                                                                                                            ===");
				System.out.println("===    javac -cp .:/ORACLE/Monitoring/Agent/lib/commons-codec-1.10.jar:/ORACLE/Monitoring/Agent/lib/ojdbc5.jar            ===");
				System.out.println("===    BkpMonAgent.java                                                                                                   ===");
				System.out.println("===  Execution :                                                                                                          ===");
				System.out.println("===    java -cp .:/ORACLE/Monitoring/Agent/lib/commons-codec-1.10.jar:/ORACLE/Monitoring/Agent/lib/ojdbc5.jar BkpMonAgent ===");
				System.out.println("===    /ORACLE/Monitoring/Agent/config/dbinfo.properties                                                                  ===");
				System.out.println("===  Parameter :                                                                                                          ===");
				System.out.println("===    args[0] /ORACLE/Monitoring/Agent/config/dbinfo.properties                                                          ===");
				System.out.println("=============================================================================================================================");
			}
			
			GetDBSrcInfo(); // Get Source DataBase Backup Info
			GetProcess(); // Get DB Backup process
			ShCmd(diskcmdstr, "DISK"); // Get Disk Size
			ShCmd(bkpcmdstr, "BACKUP"); // Get DB Backup Log Info
			PutDBTgtInfo(); // Put DB Backup Log
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// Get Source DataBase Backup Info
	public static void LoadAgentInfo(String conninfo) {

		FileInputStream fis = null;
		
		try {			
			Properties props = new Properties();
			fis = new FileInputStream(conninfo);
			props.load(new java.io.BufferedInputStream(fis));
			loglevel = props.getProperty("LogLevel");
			dbtype = props.getProperty("DBType");
			srcdbclass = props.getProperty("SrcDBClass");
			srcdburl = props.getProperty("SrcDBConnectUrl");
			srcuser = props.getProperty("SrcDBUserId");
			srcpasswd = props.getProperty("SrcDBUserPasswd");
			tgtdbclass = props.getProperty("TgtDBClass");
			tgtdburl = props.getProperty("TgtDBConnectUrl");
			tgtuser = props.getProperty("TgtDBUserId");
			tgtpasswd = DecodingChar(props.getProperty("TgtDBUserPasswd"));
			hostnm = props.getProperty("HostNm");
			agenthomedir = props.getProperty("AgentHomeDir");
			oraclebkpdir = props.getProperty("OracleBkpDir");
			oraclearcdir = props.getProperty("OracleArcDir");
			oracledbnm = props.getProperty("OracleDBNm");
			location = props.getProperty("Location");
			crontime = Integer.parseInt(props.getProperty("CronTime"));
			prcscmdstr = props.getProperty("PrcsCmd");
			diskcmdstr = props.getProperty("DiskCmd");
			bkpcmdstr = props.getProperty("BkpCmd");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("resource")
	public static void GetDBSrcInfo() {
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rssrcfile = null;
		
		String datafilelist = "SELECT FILE_NAME FROM DBA_DATA_FILES";
		String datafilecount = "SELECT COUNT(*) AS FILE_COUNT FROM DBA_DATA_FILES";
		
		try {
			if(loglevel.equals("DEBUG")) {
				System.out.println("Source DB Class : " + srcdbclass 
						         + ", Source DB Url : " + srcdburl 
						         + ", Source DB User : " + srcuser 
						         + ", Source DB Passwd : " + DecodingChar(srcpasswd));
			}

			conn = DriverManager.getConnection(srcdburl, srcuser, DecodingChar(srcpasswd));
			pstmt = conn.prepareStatement(datafilecount);
			rssrcfile = pstmt.executeQuery();

			int filecount = 0;
			
			while (rssrcfile.next()) {
				filecount = rssrcfile.getInt("FILE_COUNT");

				if(loglevel.equals("DEBUG")) {
					System.out.println("Data File Count : " + filecount);
				}
			}
			
			pstmt = conn.prepareStatement(datafilelist);
			rssrcfile = pstmt.executeQuery();
			int errorcnt = 0;
			
			while (rssrcfile.next()) {
				File chkfile = new File(rssrcfile.getString("FILE_NAME"));
				
				if(loglevel.equals("DEBUG")) {
					System.out.println("Source DB Data File : " + chkfile.getAbsoluteFile() + ", Size : " + chkfile.length());
				}
					
				File bkpdatafile = new File(oraclebkpdir+chkfile);
				
				if(loglevel.equals("DEBUG")) {
					System.out.println("Backup Data File : " + bkpdatafile.getAbsoluteFile() + ", Backup Data Size : " + bkpdatafile.length());
				}
				
				if ( bkpdatafile.length() >= chkfile.length() && bkpdatafile.length() > 0 ) {
					if(loglevel.equals("DEBUG")) {
						System.out.println("Backup is Completed!!!");
					}

					filecount--;

					hashbkpfile.put(bkpdatafile.getAbsoluteFile().toString(),
								   bkpdatafile.getAbsoluteFile().toString().substring(0, bkpdatafile.getAbsoluteFile().toString().lastIndexOf("/")) + "|" + 
                                   String.valueOf(bkpdatafile.length()) + "|Backup Completed");
				} else {
					if(loglevel.equals("DEBUG")) {
						System.out.println("Backup is Incompleted!!!");
					}

					errorcnt++;
					
					hashbkpfile.put(bkpdatafile.getAbsoluteFile().toString(), 
								   bkpdatafile.getAbsoluteFile().toString().substring(0, bkpdatafile.getAbsoluteFile().toString().lastIndexOf("/")) + "|" + 
                                   String.valueOf(bkpdatafile.length()) + "|Backup InCompleted");
				}
			}
			
			if(loglevel.equals("DEBUG")) {
				System.out.println("Error Count : " + errorcnt + ", Execute File Count : " + filecount);
			}
			
			ChkControl(oraclebkpdir);
			ChkArchive(oraclearcdir);
			
			if (chkarcfile) {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Success Archive File Backup!!!");
				}
				
				hashbkpfile.put("Archive File", oraclearcdir + "|0|Backup Completed");
			} else {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Fail Archive File Backup!!!");
				}
				
				hashbkpfile.put("Archive File", oraclearcdir + "|0|Backup InCompleted");
			}
			
			if (errorcnt != 0 || filecount != 0 || !chkctrlfile || !chkarcfile) {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Backup is InCompleted!!!");
				}
				
				MakeRsltFile(agenthomedir, oracledbnm, "F");
			} else if (errorcnt == 0 && filecount == 0 && chkctrlfile && chkarcfile) {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Backup is Completed!!!");
				}
				
				MakeRsltFile(agenthomedir, oracledbnm, "S");
			}
			
			rssrcfile.close();
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try{
				if(rssrcfile != null) { rssrcfile.close(); }				
				if(pstmt != null) { pstmt.close(); }				
				if(conn != null) { conn.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings({ "resource", "rawtypes" })
	public static void PutDBTgtInfo() {
		
		BufferedReader brd = null;
		Connection conn = null;
	    PreparedStatement pstmt = null;
		
		try {
			if(bkpmonrsltfile != null && bkpstatus == null) {
				File rsltfilechk = new File(bkpmonrsltfile);
				
				if(loglevel.equals("DEBUG")) {
					System.out.println("Backup Result File ==> " + bkpmonrsltfile);
				}
				
				brd = new BufferedReader(new FileReader(rsltfilechk));
				
				while(brd.ready()) {
					bkpstatus = brd.readLine();
					
					if(loglevel.equals("DEBUG")) {
						System.out.println("Result File Read Line ==> " + bkpstatus);
					}
					
					break;
				}
			} else {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Backup Result File is Not Exist ");
				}
			}
			
			if(arrdisklog != null) {
				for(int i = 0; i < arrdisklog.size(); i++) {
					if(loglevel.equals("DEBUG")) {
						System.out.println("Disk ArrayList ==> " + arrdisklog.get(i));
					}
					
					dfrsltstr += arrdisklog.get(i).toString() + "<br>";
				}
			}
			
			if(arrbkplog != null) {
				for(int i = 0; i < arrbkplog.size(); i++) {
					if(loglevel.equals("DEBUG")) {
						System.out.println("Backup ArrayList ==> " + arrbkplog.get(i));
					}
					
					if(arrbkplog.get(i).toString().toLowerCase().contains("kill") 
							|| arrbkplog.get(i).toString().toLowerCase().contains("terminate")) {
						
						bkpstatus = "F";
						
						if(loglevel.equals("DEBUG")) {
							System.out.println("===== CRONTAB Backup Fail, Process Kill or SYSTEM Down =====");
						}
					}
					
					bkprsltstr += arrbkplog.get(i).toString() + "<br>";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (brd != null) { brd.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			Class.forName(tgtdbclass);
		    conn = DriverManager.getConnection(tgtdburl, tgtuser, tgtpasswd);
		    
		    String delquery_master = "DELETE FROM BkpmonMaster WHERE HOSTNAME = ? AND DBNAME = ? AND REGDATE > DATE_FORMAT(?, '%Y-%m-%d %H:%i:%s')";
		    pstmt = conn.prepareStatement(delquery_master);
		    pstmt.setString(1, hostnm);
		    pstmt.setString(2, oracledbnm);
		    pstmt.setString(3, todaydate);
		    pstmt.executeUpdate();
		    
		    String delquery_slave = "DELETE FROM BkpmonSlave WHERE HOSTNAME = ? AND DBNAME = ? AND REGDATE > DATE_FORMAT(?, '%Y-%m-%d %H:%i:%s')";
		    pstmt = conn.prepareStatement(delquery_slave);
		    pstmt.setString(1, hostnm);
		    pstmt.setString(2, oracledbnm);
		    pstmt.setString(3, todaydate);
		    pstmt.executeUpdate();
		    
		    String insquery_master = 
		    		  "INSERT INTO BkpmonMaster ( "
		    		+ " HOSTNAME,                 "
		    		+ " DBNAME,                   "
		    		+ " REGDATE,                  "
		    		+ " BACKUPSTATUS,             "
		    		+ " DBTYPE,                   "
		    		+ " LOCATION,                 "
		    		+ " CRONTIME,                 "
		    		+ " DFRSLT,                   "
		    		+ " BKPRSLT)                 "
		    		+ "VALUES ( ?, ?, NOW(), ?, ?, ?, ?, ?, ? )";
		    
		    pstmt = conn.prepareStatement(insquery_master);
		    
		    if(loglevel.equals("DEBUG")) {
		    	System.out.println("BkpmonMaster Mapping Value ==> "
						   + " \n HostName : " + hostnm
						   + " \n OracleDBName : " + oracledbnm
						   + " \n BackupStatus : " + bkpstatus
						   + " \n DBType : " + dbtype
						   + " \n Location : " + location
						   + " \n CronTime : " + crontime
						   + " \n DF Result : " + dfrsltstr
						   + " \n Backup Result : " + bkprsltstr);
		    }
		    
		    pstmt.setString(1, hostnm);
		    pstmt.setString(2, oracledbnm);
		    pstmt.setString(3, bkpstatus);
		    pstmt.setString(4, dbtype);
		    pstmt.setString(5, location);
		    pstmt.setInt(6, crontime);
		    pstmt.setString(7, dfrsltstr);
		    pstmt.setString(8, bkprsltstr);
		    
		    int inscnt_master = pstmt.executeUpdate();
		    
		    String insquery_slave = 
		    		  "INSERT INTO BkpmonSlave (  "
		    		+ " HOSTNAME,                 "
		    		+ " DBNAME,                   "
		    		+ " REGDATE,                  "
		    		+ " FILEDIR,                  "
		    		+ " FILENAME,                 "
		    		+ " FILESIZE,                 "
		    		+ " MSG )                     "
		    		+ "VALUES ( ?, ?, NOW(), ?, ?, ?, ? )";
		    		
		    
		    pstmt = conn.prepareStatement(insquery_slave);
			Set sBkpFile = hashbkpfile.entrySet();
			int inscnt_slave = 0;
			
			for (Object obj : sBkpFile) {
				Map.Entry fileentry = (Map.Entry) obj;
				if(loglevel.equals("DEBUG")) {
					System.out.println("Key ==> " + fileentry.getKey() + ", Value ==> " + fileentry.getValue());
				}
				
				String[] entryval = fileentry.getValue().toString().split("\\|");
				
				if(loglevel.equals("DEBUG")) {
					System.out.println("BkpmonSlave Mapping Value ==> "
							   + " \n HostName : " + hostnm
							   + " \n OracleDBName : " + oracledbnm
							   + " \n FileDir : " + entryval[0]
							   + " \n FileName : " + fileentry.getKey().toString()
							   + " \n FileSize : " + entryval[1]
							   + " \n Message : " + entryval[2]);
				}
				
				pstmt.setString(1, hostnm);
			    pstmt.setString(2, oracledbnm);
			    pstmt.setString(3, entryval[0]);
			    pstmt.setString(4, fileentry.getKey().toString());
			    pstmt.setString(5, entryval[1]);
			    pstmt.setString(6, entryval[2]);
			    
			    inscnt_slave = pstmt.executeUpdate();
			}
			
			if (inscnt_master > 0 && inscnt_slave > 0) {
				System.out.println("Insert Success");
			} else {
				System.out.println("Insert Fail");
			}
			
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(pstmt != null) { pstmt.close(); }
				if(conn != null) { conn.close(); }				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(loglevel.equals("DEBUG")) {
			System.out.println("Host Name ==> " + hostnm + ", \n" + "DB Name ==> " + oracledbnm + ", \n" 
					 + "Backup Status (S : Success, F : Fail, P : Process) ==> " + bkpstatus + ", \n" 
					 + "DB Type ==> " + dbtype + ", \n" + "DB Location ==> " + location);
		}
	}
	
	public static String DecodingChar(String str) {
		byte[] decoded = Base64.decodeBase64(str);
		String restr = new String(decoded);
		return restr;
	}
	
	public static void ChkControl(String dir) {
		
		GetCtrlList(dir);
		File fcontrolfile = null;

		for (int i = 0; i < arrctrllist.size(); i++) {
			if(loglevel.equals("DEBUG")) {
				System.out.println("Check Control ==> " + arrctrllist.get(i).toString());
			}
			
			if(arrctrllist.get(i).toString().contains("control")) {
				fcontrolfile = new File(arrctrllist.get(i).toString());
				
				if(fcontrolfile.length() > 0) {
					hashbkpfile.put(fcontrolfile.getAbsolutePath().toString(), 
									fcontrolfile.getAbsolutePath().toString().substring(0, fcontrolfile.getAbsoluteFile().toString().lastIndexOf("/")) + "|" + String.valueOf(fcontrolfile.length()) + "|Backup Completed");
					
					if(loglevel.equals("DEBUG")) {
						System.out.println(arrctrllist.get(i).toString());
					}
					
					chkctrlfile = true;
					break;
				}
			}
		}
		
		if(!chkctrlfile) {
			hashbkpfile.put("Control File Is Not Backup", "Control File|0|Backup InCompleted");
		}
	}
	
	public static void GetCtrlList(String dir) {
		
		File ctrldir = new File(dir);
		File[] ctrllist = ctrldir.listFiles();
		
		try {
			for (int i = 0; i < ctrllist.length; i++) {
				if(ctrllist[i].isFile()) {
					arrctrllist.add(ctrllist[i].getCanonicalPath().toString());
					
					if(loglevel.equals("DEBUG")) {
						System.out.println("Get ControlList ==> " + ctrllist[i].getCanonicalPath().toString());
					}
				} else if (ctrllist[i].isDirectory()) {
					GetCtrlList(ctrllist[i].getCanonicalPath().toString());
					
					if(loglevel.equals("DEBUG")) {
						System.out.println("Get ControlList ==> " + ctrllist[i].getCanonicalPath().toString());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void ChkArchive(String dir) {
		
		try{
			Calendar cal = new GregorianCalendar(Locale.getDefault());
		    cal.setTime(new Date());
		    cal.add(Calendar.DAY_OF_YEAR, 0);
		    SimpleDateFormat fm = new SimpleDateFormat("yyyy-MM-dd");
		    String strdate = fm.format(cal.getTime());
			
			File file = new File(dir);

			if (!file.isDirectory()) {
				if(loglevel.equals("DEBUG")) {
					System.out.println("Directory is not Exists");
				}
				
				System.exit(1);
			}

			File[] list = file.listFiles();
			Arrays.sort(list, new FileModifiedDate());
			
			SimpleDateFormat transformat = new SimpleDateFormat("yyyy-MM-dd");
			Date chkdate = transformat.parse(strdate);
			
			for (File fileentry : list) {
				if (fileentry.isFile()) {
					Date filedate = new Date(fileentry.lastModified());
					String strfiledate = fm.format(filedate);
					
					Date filedate2 = transformat.parse(strfiledate);
					
					int compare = chkdate.compareTo( filedate2 );
					
					if (compare == 0) {
						if(loglevel.equals("DEBUG")) {
							System.out.println("Check Date : " + strdate + ", File Date : " + strfiledate + ", Compare Count : " + compare);
						}
						
						chkarcfile = true;
					} else {
						if(loglevel.equals("DEBUG")) {
							System.out.println("Check Date : " + strdate + ", File Date : " + strfiledate + ", Compare Count : " + compare);
						}
						
						chkarcfile = false;
					}
					
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void MakeRsltFile(String homedir, String dbname, String status) {
		
		String currtime = null;
		Date mydate = new Date();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMdd");
		mydate.setTime(System.currentTimeMillis());
		currtime = dayTime.format(mydate);
		bkpmonrsltfile = homedir + File.separator + currtime + "_" + "Rslt.txt";
		
		File frsltfile = new File(bkpmonrsltfile);
		FileOutputStream opsResult = null;
		
		try {
			if(loglevel.equals("DEBUG")) {
				System.out.println("Create Make Result File Start!!!");
			}
			
			opsResult = new FileOutputStream(frsltfile);
			opsResult.write((status).getBytes());
			opsResult.flush();
			opsResult.close();
			
			if(loglevel.equals("DEBUG")) {
				System.out.println("Create Make Result File End!!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(opsResult != null) { opsResult.close(); }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void ShCmd(String cmd, String chktype) {
		Runtime runtime = Runtime.getRuntime();
		Process process;
		
		try {
			process = runtime.exec(cmd);
			process.waitFor();
			
			BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));

			if(loglevel.equals("DEBUG")) {
				System.out.println("Backup Check Type ==> " + chktype);
			}
			
			if(chktype.equals("DISK")) {
				while(out.ready()) {
					arrdisklog.add(out.readLine());
				}
			} else if(chktype.equals("BACKUP")) {
				while(out.ready()) {
					arrbkplog.add(out.readLine());
				}
			} else {
				System.out.println("Check Type is Invalid!!!");
			}
			
			out.close();

			process.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void GetProcess() {
		String pname;
		Runtime runtime = Runtime.getRuntime();
		Process process;
		
		try {
			process = runtime.exec(prcscmdstr);
			process.waitFor();
			BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
			while ((pname = input.readLine()) != null) {
				if(loglevel.equals("DEBUG")) {
					System.out.println("DB Backup Scripts Executing, Process Name ==> " +pname);
				}
				
				bkpstatus = "P";				
			}
			
			input.close();
			process.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class FileModifiedDate implements Comparator<File> {

	public int compare(File f1, File f2) {
		if (f1.lastModified() < f2.lastModified())
			return 1;

		if (f1.lastModified() == f2.lastModified())
			return 0;

		return -1;
	}
}