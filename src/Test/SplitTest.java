package Test;

public class SplitTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String SplitSrc = "/BACKUP/DB/ORADATA/DISYSUSA|7344234496|Backup Completed";
		
		String SrcArray[] = SplitSrc.split("\\|");
		
		for (int i = 0; i < SrcArray.length; i++) {
			System.out.println(SrcArray[i]);
		}
	}

}
