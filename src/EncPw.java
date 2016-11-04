import org.apache.commons.codec.binary.Base64;

public class EncPw {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		byte[] encoded = Base64.encodeBase64(args[0].getBytes());
		
		System.out.println(new String(encoded));
	}

}
