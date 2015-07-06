package com.ctrip.hermes.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.test.broker.TestableMessageQueueStorage.DataRecord;
import com.ctrip.hermes.test.core.SettableMetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ProducerTest extends HermesBaseTest {

	private static final String TEST_TOPIC = "test_topic";

	@Test
	public void testSendSync() throws Exception {
		startBrokerMock(true);

		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		// non-priority
		sendSync(TEST_TOPIC, partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false);
		DataRecord msg1 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey);
		assertMsg(msg1, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		// priority
		sendSync(TEST_TOPIC, partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("b", "B")), true);
		DataRecord msg2 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), true), refKey);
		assertMsg(msg2, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("b", "B")));
		stopBrokerMock();
	}

	@Test
	public void testSendAsync() throws Exception {
		startBrokerMock(true);
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		Future<SendResult> future1 = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("a", "A")), false, null);
		Future<SendResult> future2 = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("b", "B")), true, null);

		future1.get();
		future2.get();

		DataRecord msg1 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey);
		DataRecord msg2 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), true), refKey);

		assertMsg(msg1, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));
		assertMsg(msg2, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("b", "B")));
		stopBrokerMock();
	}

	@Test
	public void testSendWithMetaChanged() throws Exception {
		SettableMetaHolder metaHolder = lookup(SettableMetaHolder.class);
		Endpoint endpoint = metaHolder.getMeta().findEndpoint("br0");
		endpoint.setHost("1.1.1.1");

		startBrokerMock(false);

		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		final AtomicInteger success = new AtomicInteger(-1);

		Future<SendResult> future1 = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("a", "A")), false, new CompletionCallback<SendResult>() {

			      @Override
			      public void onSuccess(SendResult result) {
				      success.set(0);
			      }

			      @Override
			      public void onFailure(Throwable t) {
				      success.set(-1);
			      }
		      });

		TimeUnit.SECONDS.sleep(3);

		Assert.assertNull(getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey));
		Assert.assertFalse(future1.isDone());

		endpoint.setHost("127.0.0.1");
		reloadMeta();

		prepareBroker();

		future1.get();

		DataRecord msg1 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey);

		assertMsg(msg1, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		Assert.assertEquals(0, success.get());

		stopBrokerMock();
	}

	@Test
	public void testSendWithCallback() throws Exception {
		startBrokerMock(true);
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		final AtomicInteger success = new AtomicInteger(-1);

		Future<SendResult> future1 = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("a", "A")), false, new CompletionCallback<SendResult>() {

			      @Override
			      public void onSuccess(SendResult result) {
				      success.set(0);
			      }

			      @Override
			      public void onFailure(Throwable t) {
				      success.set(-1);
			      }
		      });

		future1.get();

		DataRecord msg1 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey);

		assertMsg(msg1, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		Assert.assertEquals(0, success.get());

		stopBrokerMock();
	}

	@Test
	public void testSendWithRetry() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		final AtomicInteger success = new AtomicInteger(-1);

		Future<SendResult> future1 = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("a", "A")), false, new CompletionCallback<SendResult>() {

			      @Override
			      public void onSuccess(SendResult result) {
				      success.set(0);
			      }

			      @Override
			      public void onFailure(Throwable t) {
				      success.set(-1);
			      }
		      });

		TimeUnit.SECONDS.sleep(3);

		Assert.assertNull(getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey));
		Assert.assertFalse(future1.isDone());

		startBrokerMock(true);

		future1.get();

		DataRecord msg1 = getMessageStorage().getMessage(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false), refKey);

		assertMsg(msg1, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		Assert.assertEquals(0, success.get());

		stopBrokerMock();
	}

	@Test
	public void testSendWithoutRefkey() throws Exception {
		startBrokerMock(true);
		String body = "body";
		String partitionKey = "pkey";

		// non-priority
		sendSync(TEST_TOPIC, partitionKey, body, null, Arrays.asList(new Pair<String, String>("a", "A")), false);
		List<DataRecord> msgs1 = getMessageStorage().getMessages(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), false));
		System.out.println(JSON.toJSONString(msgs1));
		Assert.assertEquals(1, msgs1.size());
		DataRecord msg1 = msgs1.get(0);
		Assert.assertNotNull(msg1.getRefKey());
		assertMsg(msg1, body, msg1.getRefKey(), false, 0, Arrays.asList(new Pair<String, String>("a", "A")));

		// priority
		sendSync(TEST_TOPIC, partitionKey, body, null, Arrays.asList(new Pair<String, String>("b", "B")), true);
		List<DataRecord> msgs2 = getMessageStorage().getMessages(
		      new Tpp(TEST_TOPIC, calPartition(TEST_TOPIC, partitionKey), true));
		Assert.assertEquals(1, msgs2.size());
		DataRecord msg2 = msgs2.get(0);
		Assert.assertNotNull(msg2.getRefKey());
		assertMsg(msg2, body, msg2.getRefKey(), false, 0, Arrays.asList(new Pair<String, String>("b", "B")));
		stopBrokerMock();
	}

	@Test
	public void testSendWithoutPartitionKey() throws Exception {
		startBrokerMock(true);
		String body = "body";
		String refKey = "refKey";

		sendSync(TEST_TOPIC, null, body, refKey, Arrays.asList(new Pair<String, String>("c", "C")), true);

		Map<Integer, List<DataRecord>> messages = getMessageStorage().getMessages(TEST_TOPIC, true);
		Assert.assertEquals(1, messages.size());

		List<DataRecord> msgs = messages.values().iterator().next();

		Assert.assertEquals(1, msgs.size());

		DataRecord msg = msgs.get(0);
		assertMsg(msg, body, refKey, false, 0, Arrays.asList(new Pair<String, String>("c", "C")));

		stopBrokerMock();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendWithNullTopic() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		sendAsync(null, partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendAsyncWithLargerThan90charsRefKey() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 91; i++) {
			sb.append("c");
		}
		String refKey = sb.toString();

		sendAsync(TEST_TOPIC, partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendSyncWithLargerThan90charsRefKey() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 91; i++) {
			sb.append("c");
		}
		String refKey = sb.toString();

		sendSync(TEST_TOPIC, partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendSyncWithUnknownTopic() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		sendSync("111111", partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendAsyncWithUnknownTopic() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		sendAsync("111111", partitionKey, body, refKey, Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

	@Test(expected = TimeoutException.class)
	public void testSendAsyncTimeout() throws Exception {
		String body = "body";
		String partitionKey = "pkey";
		String refKey = "refKey";

		Future<SendResult> future = sendAsync(TEST_TOPIC, partitionKey, body, refKey,
		      Arrays.asList(new Pair<String, String>("a", "A")), false, null);
		future.get(5, TimeUnit.SECONDS);
	}

	private SendResult sendSync(String topic, String partitionKey, Object body, String refKey,
	      List<Pair<String, String>> appProperties, boolean isPriority) throws MessageSendException {
		return createMessageHolder(topic, partitionKey, body, refKey, appProperties, isPriority, null).sendSync();
	}

	private Future<SendResult> sendAsync(String topic, String partitionKey, Object body, String refKey,
	      List<Pair<String, String>> appProperties, boolean isPriority, CompletionCallback<SendResult> callback)
	      throws MessageSendException {
		return createMessageHolder(topic, partitionKey, body, refKey, appProperties, isPriority, callback).send();
	}

	private MessageHolder createMessageHolder(String topic, String partitionKey, Object body, String refKey,
	      List<Pair<String, String>> appProperties, boolean isPriority, CompletionCallback<SendResult> callback) {
		MessageHolder holder = Producer.getInstance()//
		      .message(topic, partitionKey, body)//
		      .withRefKey(refKey);

		if (appProperties != null) {
			for (Pair<String, String> pair : appProperties) {
				holder.addProperty(pair.getKey(), pair.getValue());
			}
		}

		if (callback != null) {
			holder.setCallback(callback);
		}

		if (isPriority) {
			holder.withPriority();
		}

		return holder;
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
