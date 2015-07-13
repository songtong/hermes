package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.transport.netty.Magic;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.meta.entity.Codec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultMessageCodecTest extends ComponentTestCase {

	@Before
	public void setUp() throws Exception {
		super.setUp();
		defineComponent(MetaService.class, TestMetaService.class);
	}

	@Test
	public void testEncodeWithProducerMsgAndPartialDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a", "A")), Arrays.asList(new Pair<String, String>("b", "B")),
		      Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		PartialDecodedMessage pmsg = codec.decodePartial(Unpooled.wrappedBuffer(bytes));

		Assert.assertEquals(msg.getBornTime(), pmsg.getBornTime());
		Assert.assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		Assert.assertEquals(msg.getKey(), pmsg.getKey());
		Assert.assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));
		assertProeprties(readProperties(pmsg.getDurableProperties()), Arrays.asList(new Pair<String, String>(
		      PropertiesHolder.APP + "a", "A"), new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(readProperties(pmsg.getVolatileProperties()), Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodeWithProducerMsgAndPartialDecodeWithStringBodyAndNullProperties() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true, true,
		      null, null, null);

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		PartialDecodedMessage pmsg = codec.decodePartial(Unpooled.wrappedBuffer(bytes));

		Assert.assertEquals(msg.getBornTime(), pmsg.getBornTime());
		Assert.assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		Assert.assertEquals(msg.getKey(), pmsg.getKey());
		Assert.assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));

		assertProeprties(readProperties(pmsg.getDurableProperties()), Collections.<Pair<String, String>> emptyList());
		assertProeprties(readProperties(pmsg.getVolatileProperties()), Collections.<Pair<String, String>> emptyList());
	}

	@Test
	public void testEncodeWithBufAndPartialDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a", "A")), Arrays.asList(new Pair<String, String>("b", "B")),
		      Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		ByteBuf buffer = Unpooled.buffer();

		codec.encode(msg, buffer);

		PartialDecodedMessage pmsg = codec.decodePartial(buffer);

		Assert.assertEquals(msg.getBornTime(), pmsg.getBornTime());
		Assert.assertEquals(Codec.JSON, pmsg.getBodyCodecType());
		Assert.assertEquals(msg.getKey(), pmsg.getKey());
		Assert.assertEquals(msg.getBody(), new JsonPayloadCodec().decode(pmsg.readBody(), String.class));
		assertProeprties(readProperties(pmsg.getDurableProperties()), Arrays.asList(new Pair<String, String>(
		      PropertiesHolder.APP + "a", "A"), new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(readProperties(pmsg.getVolatileProperties()), Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodePartialAndDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();

		PartialDecodedMessage pmsg = new PartialDecodedMessage();
		pmsg.setBodyCodecType(Codec.JSON);
		pmsg.setBornTime(bornTime);
		pmsg.setKey("key");
		pmsg.setWithHeader(false);
		pmsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode("topic", "hello")));
		ByteBuf durablePropsBuf = Unpooled.buffer();
		writeProperties(Arrays.asList(new Pair<String, String>("a", "A"), new Pair<String, String>("b", "B")),
		      durablePropsBuf);
		pmsg.setDurableProperties(durablePropsBuf);
		pmsg.setWithHeader(false);
		ByteBuf volatilePropsBuf = Unpooled.buffer();
		writeProperties(Arrays.asList(new Pair<String, String>("c", "C")), volatilePropsBuf);
		pmsg.setVolatileProperties(volatilePropsBuf);

		DefaultMessageCodec codec = new DefaultMessageCodec();
		ByteBuf buf = Unpooled.buffer();
		codec.encodePartial(pmsg, buf);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", buf, String.class);

		Assert.assertEquals(bornTime, cmsg.getBornTime());
		Assert.assertEquals("hello", cmsg.getBody());
		Assert.assertEquals("key", cmsg.getRefKey());
		Assert.assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(),
		      Arrays.asList(new Pair<String, String>("a", "A"), new Pair<String, String>("b", "B")));
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
		      Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test
	public void testEncodePartialAndDecodeWithStringBodyAndNullProperties() throws Exception {
		long bornTime = System.currentTimeMillis();

		PartialDecodedMessage pmsg = new PartialDecodedMessage();
		pmsg.setBodyCodecType(Codec.JSON);
		pmsg.setBornTime(bornTime);
		pmsg.setKey("key");
		pmsg.setWithHeader(false);
		pmsg.setBody(Unpooled.wrappedBuffer(new JsonPayloadCodec().encode("topic", "hello")));
		pmsg.setDurableProperties(null);
		pmsg.setWithHeader(false);
		pmsg.setVolatileProperties(null);

		DefaultMessageCodec codec = new DefaultMessageCodec();
		ByteBuf buf = Unpooled.buffer();
		codec.encodePartial(pmsg, buf);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", buf, String.class);

		Assert.assertEquals(bornTime, cmsg.getBornTime());
		Assert.assertEquals("hello", cmsg.getBody());
		Assert.assertEquals("key", cmsg.getRefKey());
		Assert.assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(),
		      Collections.<Pair<String, String>> emptyList());
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
		      Collections.<Pair<String, String>> emptyList());
	}

	@Test
	public void testEncodeAndDecodeWithStringBody() throws Exception {
		long bornTime = System.currentTimeMillis();
		ProducerMessage<String> msg = createProducerMessage("topic", "body", "key", 10, 10, "pKey", bornTime, true, true,
		      Arrays.asList(new Pair<String, String>("a", "A")), Arrays.asList(new Pair<String, String>("b", "B")),
		      Arrays.asList(new Pair<String, String>("c", "C")));

		DefaultMessageCodec codec = new DefaultMessageCodec();

		byte[] bytes = codec.encode(msg);

		BaseConsumerMessage<?> cmsg = codec.decode("topic", Unpooled.wrappedBuffer(bytes), String.class);

		Assert.assertEquals(bornTime, cmsg.getBornTime());
		Assert.assertEquals("body", cmsg.getBody());
		Assert.assertEquals("key", cmsg.getRefKey());
		Assert.assertEquals("topic", cmsg.getTopic());

		assertProeprties(cmsg.getPropertiesHolder().getDurableProperties(), Arrays.asList(new Pair<String, String>(
		      PropertiesHolder.APP + "a", "A"), new Pair<String, String>(PropertiesHolder.SYS + "b", "B")));
		assertProeprties(cmsg.getPropertiesHolder().getVolatileProperties(),
		      Arrays.asList(new Pair<String, String>("c", "C")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMagicNumberCheckFail() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		buf.writeBytes(new byte[] { 1, 2, 3, 4 });
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknowVersion() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);
		buf.writeByte(100);
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCRCFail() throws Exception {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);
		buf.writeByte(MessageCodecVersion.BINARY_V1.getVersion());
		buf.writeInt(30);
		buf.writeInt(1);
		buf.writeInt(1);
		buf.writeBytes(new byte[] { 1, 2, 1 });
		buf.writeLong(10L);
		MessageCodec codec = new DefaultMessageCodec();
		codec.decode("topic", buf, String.class);
	}

	private ProducerMessage<String> createProducerMessage(String topic, String body, String key, int seq, int partition,
	      String partitionKey, long bornTime, boolean isPriority, boolean withHeader,
	      List<Pair<String, String>> appProperites, List<Pair<String, String>> sysProperites,
	      List<Pair<String, String>> volatileProperites) {
		ProducerMessage<String> msg = new ProducerMessage<String>(topic, body);
		msg.setBornTime(bornTime);
		msg.setKey(key);
		msg.setMsgSeqNo(seq);
		msg.setPartition(partition);
		msg.setPartitionKey(partitionKey);
		msg.setPriority(isPriority);
		msg.setWithHeader(withHeader);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		if (appProperites != null && !appProperites.isEmpty()) {
			for (Pair<String, String> appProperty : appProperites) {
				propertiesHolder.addDurableAppProperty(appProperty.getKey(), appProperty.getValue());
			}
		}
		if (sysProperites != null && !sysProperites.isEmpty()) {
			for (Pair<String, String> sysProperty : sysProperites) {
				propertiesHolder.addDurableSysProperty(sysProperty.getKey(), sysProperty.getValue());
			}
		}
		if (volatileProperites != null && !volatileProperites.isEmpty()) {
			for (Pair<String, String> volatileProperty : volatileProperites) {
				propertiesHolder.addVolatileProperty(volatileProperty.getKey(), volatileProperty.getValue());
			}
		}
		msg.setPropertiesHolder(propertiesHolder);
		return msg;
	}

	private void assertProeprties(Map<String, String> actual, List<Pair<String, String>> expects) {
		if (expects == null) {
			Assert.assertNull(actual);
		} else {
			Assert.assertEquals(actual.size(), expects.size());
			for (Pair<String, String> expPair : expects) {
				Assert.assertEquals(expPair.getValue(), actual.get(expPair.getKey()));
			}
		}
	}

	private void writeProperties(List<Pair<String, String>> properties, ByteBuf buf) {
		if (properties != null && !properties.isEmpty()) {

			Map<String, String> map = new HashMap<>();
			for (Pair<String, String> prop : properties) {
				map.put(prop.getKey(), prop.getValue());
			}

			HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
			codec.writeStringStringMap(map);
		}
	}

	private Map<String, String> readProperties(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
		return codec.readStringStringMap();
	}

	public static class TestMetaService extends DefaultMetaService {

		@Override
		public Codec findCodecByTopic(String topicName) {
			return new Codec(Codec.JSON);
		}

		@Override
		public void initialize() throws InitializationException {
			// do nothing
		}

	}

}
