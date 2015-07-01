package com.ctrip.hermes.test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.test.broker.TestableMessageQueueStorage.DataRecord;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ProducerTest extends HermesBaseTest {

	private static String m_topic = "test_topic";

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		startBrokerMock();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		stopBrokerMock();
		super.tearDown();
	}

	@Test
	public void testSendSync() throws Exception {
		String body = "body";
		String parititionKey = "pkey";
		String refKey = "refKey";

		Producer.getInstance()//
		      .message(m_topic, parititionKey, body)//
		      .withRefKey(refKey)//
		      .addProperty("a", "A")//
		      .sendSync();

		DataRecord msg = getMessageStorage().getMessage(new Tpp(m_topic, calPartition(m_topic, parititionKey), false),
		      refKey);

		assertMsg(msg, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		Producer.getInstance()//
		      .message(m_topic, parititionKey, body)//
		      .withPriority()//
		      .withRefKey(refKey)//
		      .addProperty("b", "B")//
		      .sendSync();

		DataRecord pmsg = getMessageStorage().getMessage(new Tpp(m_topic, calPartition(m_topic, parititionKey), true),
		      refKey);

		assertMsg(pmsg, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("b", "B")));
	}

	@Test
	public void testSendAsync() throws Exception {
		String body = "body";
		String parititionKey = "pkey";
		String refKey = "refKey";

		Future<SendResult> future1 = Producer.getInstance()//
		      .message(m_topic, parititionKey, body)//
		      .withRefKey(refKey)//
		      .addProperty("a", "A")//
		      .send();

		Future<SendResult> future2 = Producer.getInstance()//
		      .message(m_topic, parititionKey, body)//
		      .withPriority()//
		      .withRefKey(refKey)//
		      .addProperty("b", "B")//
		      .send();

		future1.get();
		future2.get();

		DataRecord msg = getMessageStorage().getMessage(new Tpp(m_topic, calPartition(m_topic, parititionKey), false),
		      refKey);

		assertMsg(msg, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		DataRecord pmsg = getMessageStorage().getMessage(new Tpp(m_topic, calPartition(m_topic, parititionKey), true),
		      refKey);

		assertMsg(pmsg, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("b", "B")));
	}

	private void assertMsg(DataRecord msg, Object body, String refKey, boolean isResend, int remainingRetries,
	      List<Pair<String, String>> appProperties) {
		Assert.assertEquals(body, decodeBody(msg, body.getClass()));
		Assert.assertEquals(refKey, msg.getRefKey());
		Assert.assertEquals(isResend, msg.isResend());
		Assert.assertEquals(remainingRetries, msg.getRemainingRetries());
		if (appProperties != null) {
			for (Pair<String, String> pair : appProperties) {
				Assert.assertEquals(pair.getValue(), getDurableAppProperty(msg, pair.getKey()));
			}
		}
	}
}
