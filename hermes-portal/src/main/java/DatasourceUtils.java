import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.unidal.helper.Bytes;
import org.unidal.helper.Codes;

public class DatasourceUtils {
         public static String encode(String src) throws Exception {
                   int p = new Random().nextInt(5) + 3;

                   return String.format("~{%s}", encode(src, p, p / 2 + 1, p * 2 + 1));
         }

         private static String encode(String src, int p, int q, int k) throws Exception {
                   byte[] data = padding(src);

                   Bytes.forBits().swap(data, p, q);
                   Bytes.forBits().mask(data, k);

                   return wrapup(data, p, q, k);
         }

         private static byte[] padding(String str) throws Exception {
                   byte[] data = str.getBytes("utf-8");
                   ByteBuffer bb = ByteBuffer.allocate(data.length + 13);

                   bb.put(data);
                   bb.put((byte) 0);
                   bb.put(Inet4Address.getLocalHost().getAddress());
                   bb.putLong(System.currentTimeMillis());

                   return (byte[]) bb.flip().array();
         }

         private static String wrapup(byte[] data, int p, int q, int k) {
                   StringBuilder sb = new StringBuilder(data.length * 2 + 3);

                   sb.append(Integer.toHexString(p | 0x8));
                   sb.append(Integer.toHexString(q));
                   sb.append(Integer.toHexString(k));

                   for (byte d : data) {
                            sb.append(Integer.toHexString(d >> 4 & 0xF));
                            sb.append(Integer.toHexString(d & 0xF));
                   }

                   return sb.toString();
         }

         public static String decode(String src) {
                   return Codes.forDecode().decode(src.substring(2, src.length() - 1));
         }

         public static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

         public static Date parseTime(String formatted) {
                   try {
                            return formatter.parse(formatted);
                   } catch (ParseException e) {
                            return null;
                   }
         }

         public static String formatTime(Date date) {
                   return formatter.format(date);
         }

         public static void main(String[] args) throws Exception {
                   System.out.println(decode("~{b276410422738825fd64f62628301bede0c7643401e0725271203070743999afed902}"));
                   //System.out.println(encode("6VdP7NslfNgrKdhQ9RSC"));
         }
}
