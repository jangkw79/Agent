package Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDate {
	
	public static void main(String args[]) {
		getDate();
	}
	
	public static final String getDate() {

		String currTime = null;
		Date myDate = new Date();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMdd");
		myDate.setTime(System.currentTimeMillis());
		currTime = dayTime.format(myDate);

		System.out.println(currTime);
		return currTime;
	}
}
