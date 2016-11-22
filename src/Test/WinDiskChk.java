package Test;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;

public class WinDiskChk {

	public static void main(String[] args) {
		
		NumberFormat nf = NumberFormat.getNumberInstance();
		
		for (Path root : FileSystems.getDefault().getRootDirectories()) {

		    System.out.print(root + ": ");
		    try {
		        FileStore store = Files.getFileStore(root);
		        System.out.println("available=" + nf.format(store.getUsableSpace()) + ", total=" + nf.format(store.getTotalSpace()));
		    } catch (IOException e) {
		        System.out.println("error querying space: " + e.toString());
		    }
		}
	}

}
