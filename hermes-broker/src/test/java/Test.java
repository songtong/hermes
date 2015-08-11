

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Test {

	public static void main(String[] args) {
		startRawMySQLTest();
	}

	private static void startRawMySQLTest() {
		new Thread() {
			public void run() {
				try {
					Class.forName("com.mysql.jdbc.Driver");
					Connection conn = DriverManager.getConnection("jdbc:mysql://10.2.5.230/FxHermesShard01DB", "root",
					      "root");
					// Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/FxHermesShard01DB", "root", "");
					String sql = "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.attributes,mp.codec_type,mp.creation_date,mp.payload FROM 1_1_message_1 mp WHERE mp.id > ? LIMIT 50";
					// String sql =
					// "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.attributes,mp.codec_type,mp.creation_date FROM 1_1_message_1 mp WHERE mp.id > ? LIMIT 50";
					PreparedStatement stmt = conn.prepareStatement(sql);

					int startId = 0;
					while (true) {
						// if (startId % 500 == 0) {
						System.out.println(startId);
						// }
						stmt.setLong(1, startId);
						long start = System.currentTimeMillis();
						System.out.println(">>>>>");
						ResultSet rs = stmt.executeQuery();
						System.out.println("<<<<");
						while (rs.next()) {
							rs.getLong(1);
							rs.getString(2);
							rs.getInt(3);
							rs.getString(4);
							rs.getBytes(5);
							rs.getString(6);
							rs.getDate(7);
							rs.getBytes(8);
						}

						long cost = System.currentTimeMillis() - start;
						if (cost > 1000) {
							System.out.println(String.format("RAW FindIdAfter %s takes %s", startId, cost));
						}

						startId += 50;
						startId %= 600000;
						Thread.sleep(500);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}.start();
	}

}
