package com.ctrip.hermes.broker.perf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageBatchWithRawData;
import com.ctrip.hermes.core.transport.command.parser.DefaultCommandParser;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.google.common.util.concurrent.SettableFuture;

public class MySQLPerfTest {

	public static void main(String[] args) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000; i++) {
			sb.append("x");
		}
		String mainBody = sb.toString();

		long start = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			saveOneBatch(mainBody);
		}
		System.out.println(System.currentTimeMillis() - start);
	}

	public static void saveOneBatch(String mainBody) throws Exception {
		Tpp tpp = new Tpp("order_new", 1, false);

		SendMessageCommand cmd = new SendMessageCommand(tpp.getTopic(), tpp.getPartition());
		for (int i = 0; i < 10000; i++) {
			String body = i + ":order_new " + mainBody;
			String key = i + ":order_new ";
			ProducerMessage<?> msg = createProducerMessage(tpp.getTopic(), body, key, tpp.getPartition());
			SettableFuture<SendResult> future = SettableFuture.create();
			cmd.addMessage(msg, future);
		}

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodecCmd = (SendMessageCommand) new DefaultCommandParser().parse(buf);

		appendMessages(tpp, decodecCmd.getMessageRawDataBatches().values());
	}

	static ProducerMessage<String> createProducerMessage(String topic, String body, String key, int partition) {
		ProducerMessage<String> msg = new ProducerMessage<String>(topic, body);
		msg.setBornTime(System.currentTimeMillis());
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPriority(false);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		msg.setPropertiesHolder(propertiesHolder);
		return msg;
	}

	static void appendMessages(Tpp tpp, Collection<MessageBatchWithRawData> batches) throws Exception {
		List<MessagePriority> msgs = new ArrayList<>();
		for (MessageBatchWithRawData batch : batches) {
			List<PartialDecodedMessage> pdmsgs = batch.getMessages();
			for (PartialDecodedMessage pdmsg : pdmsgs) {
				MessagePriority msg = new MessagePriority();
				msg.setAttributes(pdmsg.readDurableProperties());
				msg.setCreationDate(new Date(pdmsg.getBornTime()));
				msg.setPartition(tpp.getPartition());
				msg.setPayload(pdmsg.readBody());
				msg.setPriority(tpp.isPriority() ? 0 : 1);
				msg.setProducerId(0);
				msg.setProducerIp("");
				msg.setRefKey(pdmsg.getKey());
				msg.setTopic(tpp.getTopic());
				msg.setCodecType(pdmsg.getBodyCodecType());

				msgs.add(msg);
			}
		}

		long start = System.currentTimeMillis();
		MessagePriority[] array = msgs.toArray(new MessagePriority[msgs.size()]);
		System.out.println("ToArray: " + (System.currentTimeMillis() - start));
		start = System.currentTimeMillis();
		PlexusComponentLocator.lookup(MessagePriorityDao.class).insert(array);
		System.out.println("Insert: " + (System.currentTimeMillis() - start));
	}

	private static void startRawMySQLTest() {
		new Thread() {
			public void run() {
				try {
					Class.forName("com.mysql.jdbc.Driver");
					// Connection conn = DriverManager.getConnection("jdbc:mysql://10.2.5.230/meta", "root", "root");
					Connection conn = DriverManager.getConnection("jdbc:mysql://10.2.5.230/FxHermesShard01DB", "root",
					      "root");
					// Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/FxHermesShard01DB", "root", "");
					// Connection conn =
					// DriverManager.getConnection("jdbc:mysql://pub.mysql.db.dev.sh.ctripcorp.com:28747/FxHermesShard01DB",
					// "us_dev_hermes", "hermes123456");
					String sql = "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.codec_type,mp.creation_date,mp.attributes,mp.payload FROM 1_1_message_1 mp WHERE mp.id > ? LIMIT 50";
					// String sql =
					// "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.codec_type,mp.creation_date,mp.attributes FROM 1_1_message_1 mp WHERE mp.id > ? LIMIT 50";
					// String sql =
					// "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.codec_type,mp.creation_date,mp.attributes,mp.payload FROM 900797_1_message_1 mp WHERE mp.id > ? LIMIT 50";
					// String sql =
					// "SELECT mp.id,mp.producer_ip,mp.producer_id,mp.ref_key,mp.codec_type,mp.creation_date FROM 1_0_message_0 mp WHERE mp.id > ? LIMIT 50";
					// String sql = "SELECT id,value FROM meta mp WHERE mp.id > ? LIMIT 50";
					PreparedStatement stmt = conn.prepareStatement(sql);

					int startId = 0;
					while (true) {
						// if (startId % 500 == 0) {
						System.out.println(startId);
						// }
						stmt.setLong(1, startId);
						long start = System.currentTimeMillis();
						// System.out.println(">>>>>");
						ResultSet rs = stmt.executeQuery();
						// System.out.println("<<<<");
						while (rs.next()) {
							rs.getLong(1);
							rs.getString(2);
							rs.getInt(3);
							rs.getString(4);
							rs.getString(5);
							rs.getDate(6);
							rs.getBytes(7);
							rs.getBytes(8);
						}
						rs.close();

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
