package Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ShellCommander {
	public static void main(String[] args) {
		String command = "ls -al";
		try {
			shellCmd(command);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void shellCmd(String command) {
		Runtime runtime = Runtime.getRuntime();
		Process process;
		
		try {
			process = runtime.exec(command);

			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;
			
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}