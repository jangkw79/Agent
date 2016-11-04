package Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class GetProcess {
	public static void main(String[] args) {
		try {
			String process;
			// getRuntime: Returns the runtime object associated with the current Java application.
			// exec: Executes the specified string command in a separate process.
			Process p = Runtime.getRuntime().exec("ps -ef | grep -i oracle | grep -i cp");
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((process = input.readLine()) != null) {
				System.out.println(process); // <-- Print all Process here line
												// by line
			}
			input.close();
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}