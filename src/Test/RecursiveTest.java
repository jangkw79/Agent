package Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RecursiveTest {

	static HashMap<String, String> arrBkpFile = new HashMap<String, String>();
	static ArrayList<String> arrctrllist = new ArrayList<String>();
	static boolean bChkcontrol = false;
	
	public static void main(String[] args) {
		ChkControl("D:/ControlCheck");
		//ChkControl("/BACKUP/DB");
		if(bChkcontrol) {
			System.out.println("CheckControl True");
			
			Set sBkpFile = arrBkpFile.entrySet();
			
			for (Object obj : sBkpFile) {
				Map.Entry fileentry = (Map.Entry) obj;
				System.out.println("Key ==> " + fileentry.getKey() + ", Value ==> " + fileentry.getValue());
			}
			
		} else {
			System.out.println("CheckControl False");
		}
	}
	
	public static void ChkControl(String dir) {
		
		ArrayList<String> arrctrltmplist = null;
		arrctrltmplist = GetCtrlList(dir);
		File fcontrolfile = null;
		
		for (int i = 0; i < arrctrltmplist.size(); i++) {
			if(arrctrltmplist.get(i).toString().contains("control")) {
				fcontrolfile = new File(arrctrltmplist.get(i).toString());
				if(fcontrolfile.length() > 0) {
					arrBkpFile.put("Control File", dir + arrctrltmplist.get(i).toString() + "|Backup Completed");
					System.out.println(arrctrltmplist.get(i).toString());
					bChkcontrol = true;
					break;
				}
			}
		}
	}
	
	public static ArrayList<String> GetCtrlList(String dir) {
		
		File ctrldir = new File(dir);
		File[] ctrllist = ctrldir.listFiles();
		
		try {
			for (int i = 0; i < ctrllist.length; i++) {
				if(ctrllist[i].isFile()) {
					arrctrllist.add(ctrllist[i].getCanonicalPath().toString());
					System.out.println(ctrllist[i].getCanonicalPath().toString());
				} else if (ctrllist[i].isDirectory()) {
					GetCtrlList(ctrllist[i].getCanonicalPath().toString());
					System.out.println(ctrllist[i].getCanonicalPath().toString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return arrctrllist;
	}
}
