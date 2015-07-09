package com.ctrip.hermes.producer;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.producer.config.ProducerConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ProducerIntegrationTest extends BaseProducerIntegrationTest {

	private static final String TEST_TOPIC = "test_topic";

	@Test
	public void testSendPriorityAsync() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		future.get();
		Assert.assertTrue(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertNull(msgs.get(0));
		// non-priority
		Assert.assertEquals(1, msgs.get(1).size());

		assertMsg(msgs.get(1).get(0), TEST_TOPIC, "pKey", "body", "rKey", appProperties);
	}

	@Test
	public void testSendNonPriorityAsync() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, true, null);

		future.get();
		Assert.assertTrue(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", "rKey", appProperties);
	}

	@Test
	public void testSendPrioritySync() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		sendSync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertNull(msgs.get(0));
		// non-priority
		Assert.assertEquals(1, msgs.get(1).size());

		assertMsg(msgs.get(1).get(0), TEST_TOPIC, "pKey", "body", "rKey", appProperties);
	}

	@Test
	public void testSendNonPrioritySync() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", "rKey", appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithEmptyRefKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "pKey", "body", "", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", null, appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithNullRefKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "pKey", "body", null, appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", null, appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithWhiteSpaceRefKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "pKey", "body", "             ", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", null, appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithEmptyPartitionKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "", "body", "rKey", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, null, "body", "rKey", appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithNullPartitionKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, null, "body", "rKey", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, null, "body", "rKey", appProperties);
	}

	@Test
	public void testSendNonPrioritySyncWithWhiteSpacePartitionKey() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		sendSync(TEST_TOPIC, "               ", "body", "rKey", appProperties, true);

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, null, "body", "rKey", appProperties);
	}

	@Test
	public void testSendWithCallback() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseSucessResult//
		);

		final AtomicInteger success = new AtomicInteger(-2);
		final CountDownLatch latch = new CountDownLatch(1);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("b", "B"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, true,
		      new CompletionCallback<SendResult>() {

			      @Override
			      public void onSuccess(SendResult result) {
				      latch.countDown();
				      success.set(0);
			      }

			      @Override
			      public void onFailure(Throwable t) {
				      latch.countDown();
				      success.set(-1);
			      }
		      });

		future.get();
		Assert.assertTrue(future.isDone());
		latch.await();
		Assert.assertEquals(0, success.get());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());

		SendMessageCommand sendCmd = (SendMessageCommand) brokerReceivedCmds.get(0);
		ConcurrentMap<Integer, List<ProducerMessage<?>>> msgs = sendCmd.getMsgs();
		// priority
		Assert.assertEquals(1, msgs.get(0).size());
		// non-priority
		Assert.assertNull(msgs.get(1));

		assertMsg(msgs.get(0).get(0), TEST_TOPIC, "pKey", "body", "rKey", appProperties);
	}

	@Test
	public void testSendWithBrokerNoResponse() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		MessageSendAnswer.NoOp);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		try {
			future.get(lookup(ProducerConfig.class).getDefaultBrokerSenderSendTimeoutMillis() + 1000L,
			      TimeUnit.MILLISECONDS);
			Assert.fail();
		} catch (TimeoutException e) {
			// do nothing
		} catch (Exception e) {
			Assert.fail();
		}
		Assert.assertFalse(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertTrue(brokerReceivedCmds.size() > 1);

		SendMessageCommand sendCmd = null;
		for (Command cmd : brokerReceivedCmds) {
			Assert.assertTrue(cmd instanceof SendMessageCommand);
			if (sendCmd == null) {
				sendCmd = (SendMessageCommand) cmd;
			} else {
				Assert.assertTrue(cmd == sendCmd);
			}
		}
	}

	@Test
	public void testSendWithBrokerNotAccept() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		MessageSendAnswer.NotAccept);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		try {
			future.get(lookup(ProducerConfig.class).getDefaultBrokerSenderSendTimeoutMillis() + 1000L,
			      TimeUnit.MILLISECONDS);
			Assert.fail();
		} catch (TimeoutException e) {
			// do nothing
		} catch (Exception e) {
			Assert.fail();
		}
		Assert.assertFalse(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertTrue(brokerReceivedCmds.size() > 1);

		SendMessageCommand sendCmd = null;
		for (Command cmd : brokerReceivedCmds) {
			Assert.assertTrue(cmd instanceof SendMessageCommand);
			if (sendCmd == null) {
				sendCmd = (SendMessageCommand) cmd;
			} else {
				Assert.assertTrue(cmd == sendCmd);
			}
		}
	}

	@Test
	public void testSendWithBrokerAcceptButNoResponse() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept,//
		      MessageSendAnswer.NoOp);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		try {
			future.get(lookup(ProducerConfig.class).getDefaultBrokerSenderSendTimeoutMillis() + 1000L,
			      TimeUnit.MILLISECONDS);
			Assert.fail();
		} catch (TimeoutException e) {
			// do nothing
		} catch (Exception e) {
			Assert.fail();
		}
		Assert.assertFalse(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());
	}

	@Test
	public void testSendAsyncWithFailResponse() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseFailResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		try {
			future.get();
			Assert.fail();
		} catch (ExecutionException e) {
			if (!(e.getCause() instanceof MessageSendException)) {
				Assert.fail();
			}
		} catch (Exception e) {
			Assert.fail();
		}
		Assert.assertTrue(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());
	}

	@Test
	public void testSendSyncWithFailResponse() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		      MessageSendAnswer.Accept, //
		      MessageSendAnswer.ResponseFailResult//
		);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));

		try {
			sendSync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false);
			Assert.fail();
		} catch (MessageSendException e) {
			// do nothing
		} catch (Exception e) {
			Assert.fail();
		}

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(1, brokerReceivedCmds.size());
	}

	@Test
	public void testSendWithTaskQueueFull() throws Exception {
		brokerActionsWhenReceivedSendMessageCmd(//
		MessageSendAnswer.NoOp //
		);
		int times = Integer.valueOf(lookup(ProducerConfig.class).getDefaultBrokerSenderTaskQueueSize()) + 2;
		List<Future<SendResult>> futures = new ArrayList<>(times);
		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		for (int i = 0; i < times; i++) {
			futures.add(sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null));
			if (i == 0) {
				TimeUnit.MILLISECONDS.sleep(Integer.valueOf(lookup(ProducerConfig.class)
				      .getDefaultBrokerSenderNetworkIoCheckIntervalMaxMillis()) + 50);
			}
		}

		for (int i = 0; i < times - 1; i++) {
			Assert.assertFalse(futures.get(i).isDone());
		}

		try {
			futures.get(times - 1).get();
			Assert.fail();
		} catch (ExecutionException e) {
			if (!(e.getCause() instanceof MessageSendException)) {
				Assert.fail();
			}
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testSendWithNoEndpoint() throws Exception {
		Meta meta = m_metaHolder.getMeta();
		reset(m_metaHolder);
		Topic topic = meta.findTopic(TEST_TOPIC);
		for (Partition p : topic.getPartitions()) {
			p.setEndpoint(null);
		}
		when(m_metaHolder.getMeta()).thenReturn(meta);

		brokerActionsWhenReceivedSendMessageCmd(//
		MessageSendAnswer.NoOp);

		List<Pair<String, String>> appProperties = Arrays.asList(new Pair<String, String>("a", "A"));
		Future<SendResult> future = sendAsync(TEST_TOPIC, "pKey", "body", "rKey", appProperties, false, null);

		try {
			future.get(lookup(ProducerConfig.class).getDefaultBrokerSenderSendTimeoutMillis() + 1000L,
			      TimeUnit.MILLISECONDS);
			Assert.fail();
		} catch (TimeoutException e) {
			// do nothing
		} catch (Exception e) {
			Assert.fail();
		}
		Assert.assertFalse(future.isDone());

		List<Command> brokerReceivedCmds = getBrokerReceivedCmds();
		Assert.assertEquals(0, brokerReceivedCmds.size());

	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendWithNullTopic() throws Exception {
		sendAsync(null, "pKey", "body", "rKey", Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendAsyncWithRefKeyLargerThan90Chars() throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 91; i++) {
			sb.append("c");
		}
		String refKey = sb.toString();

		sendAsync(TEST_TOPIC, "pkey", "body", refKey, Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendSyncWithRefKeyLargerThan90Chars() throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 91; i++) {
			sb.append("c");
		}
		String refKey = sb.toString();

		sendSync(TEST_TOPIC, "pKey", "body", refKey, Arrays.asList(new Pair<String, String>("a", "A")), false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendSyncWithUnknownTopic() throws Exception {
		sendSync("111111", "pKey", "body", "rKey", Arrays.asList(new Pair<String, String>("a", "A")), false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSendAsyncWithUnknownTopic() throws Exception {
		sendAsync("111111", "pKey", "body", "rKey", Arrays.asList(new Pair<String, String>("a", "A")), false, null);
	}

}
