package com.ctrip.hermes.portal.resource.assists;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import org.codehaus.plexus.util.Base64;

public class ValidationUtils {
	private static final String DES = "DES";
	public static final String UTF8 = "utf-8";
	public static byte[] key = "12345678".getBytes();

	public static String encode(String input) throws Exception {
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
		DESKeySpec desKey = new DESKeySpec(key);
		SecretKey securekey = keyFactory.generateSecret(desKey);

		Cipher cipher = Cipher.getInstance(DES);
		cipher.init(Cipher.ENCRYPT_MODE, securekey);

		return new String(Base64.encodeBase64(cipher.doFinal(input.getBytes(UTF8))), UTF8);

	}

	public static String decode(String input) throws Exception, IllegalBlockSizeException, BadPaddingException {
		SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
		DESKeySpec desKey = new DESKeySpec(key);
		SecretKey securekey = keyFactory.generateSecret(desKey);

		Cipher cipher = Cipher.getInstance(DES);
		cipher.init(Cipher.DECRYPT_MODE, securekey);

		return new String(cipher.doFinal(Base64.decodeBase64(input.getBytes(UTF8))), UTF8);

	}

}
