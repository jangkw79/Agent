import org.apache.commons.codec.binary.Base64;

public class DecPw {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		byte[] decoded = Base64.decodeBase64(args[0]);
		
		System.out.println(new String(decoded));
	}

}
