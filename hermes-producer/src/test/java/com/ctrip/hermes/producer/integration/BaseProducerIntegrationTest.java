package com.ctrip.hermes.producer.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.message.partition.PartitioningStrategy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.ctrip.hermes.producer.monitor.DefaultSendMessageResultMonitor;
import com.ctrip.hermes.producer.monitor.SendMessageAcceptanceMonitor;
import com.ctrip.hermes.producer.monitor.SendMessageResultMonitor;
import com.ctrip.hermes.producer.sender.BrokerMessageSender;
import com.ctrip.hermes.producer.sender.MessageSender;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseProducerIntegrationTest extends ComponentTestCase {

	private List<Command> m_receivedCmds = new ArrayList<Command>();

	@Mock
	protected EndpointClient m_endpointClient;

	@Mock
	protected TestMetaHolder m_metaHolder;

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		configureStubComponents();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
	}

	private void configureStubComponents() throws Exception {

		defineComponent(ProducerConfig.class, TestProducerConfig.class)//
		      .req(ClientEnvironment.class);

		defineComponent(MessageSender.class, Endpoint.BROKER, TestMessageSender.class)//
		      .req(ProducerConfig.class)//
		      .req(SystemClockService.class)//
		      .req(EndpointManager.class)//
		      .req(PartitioningStrategy.class)//
		      .req(MetaService.class)//
		      .req(SendMessageAcceptanceMonitor.class)//
		      .req(SendMessageResultMonitor.class);

		defineComponent(MetaService.class, TestMetaService.class);

		defineComponent(SendMessageResultMonitor.class, TestSendMessageResultMonitor.class);

		((TestMessageSender) lookup(MessageSender.class, Endpoint.BROKER)).setEndpointClient(m_endpointClient);
		((TestMetaService) lookup(MetaService.class)).setMetaHolder(m_metaHolder);

		when(m_metaHolder.getMeta()).thenReturn(loadLocalMeta());
	}

	private Meta loadLocalMeta() throws Exception {
		String fileName = getClass().getSimpleName() + "-meta.xml";
		InputStream in = getClass().getResourceAsStream(fileName);

		if (in == null) {
			throw new RuntimeException(String.format("File %s not found in classpath.", fileName));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (Exception e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			}
		}
	}

	protected SendResult sendSync(String topic, String partitionKey, Object body, String refKey,
	      List<Pair<String, String>> appProperties, boolean isPriority) throws MessageSendException {
		return createMessageHolder(topic, partitionKey, body, refKey, appProperties, isPriority, null).sendSync();
	}

	protected Future<SendResult> sendAsync(String topic, String partitionKey, Object body, String refKey,
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

	protected void brokerActionsWhenReceivedSendMessageCmd(Answer<?>... answers) {
		doAnswer(new CompositeAnswer(answers))//
		      .when(m_endpointClient)//
		      .writeCommand(any(Endpoint.class), //
		            any(SendMessageCommand.class), //
		            anyLong(), //
		            any(TimeUnit.class));
	}

	protected List<Command> getBrokerReceivedCmds() {
		return new ArrayList<Command>(m_receivedCmds);
	}

	protected void assertMsg(ProducerMessage<?> msg, String topic, String partitionKey, Object body, String refKey,
	      List<Pair<String, String>> appProperties) {
		assertEquals(topic, msg.getTopic());
		assertEquals(body, msg.getBody());
		if (refKey != null) {
			assertEquals(refKey, msg.getKey());
		} else {
			assertFalse(StringUtils.isEmpty(msg.getKey()));
		}
		if (partitionKey != null) {
			assertEquals(partitionKey, msg.getPartitionKey());
		} else {
			assertFalse(StringUtils.isEmpty(msg.getPartitionKey()));
		}
		Map<String, String> durableProperties = msg.getPropertiesHolder().getDurableProperties();
		if (appProperties == null) {
			for (String key : durableProperties.keySet()) {
				assertFalse(key.startsWith(PropertiesHolder.APP));
			}
		} else {
			for (Pair<String, String> kv : appProperties) {
				String key = kv.getKey();
				String value = kv.getValue();
				assertEquals(value, durableProperties.get(PropertiesHolder.APP + key));
			}
		}
	}

	private class CompositeAnswer implements Answer<Void> {
		private List<Answer<?>> m_answers = new LinkedList<Answer<?>>();

		public CompositeAnswer(Answer<?>... answers) {
			if (answers != null && answers.length > 0) {
				m_answers.addAll(Arrays.asList(answers));
			}
		}

		@Override
		public Void answer(InvocationOnMock invocation) throws Throwable {
			m_receivedCmds.add(invocation.getArgumentAt(1, Command.class));
			for (Answer<?> answer : m_answers) {
				answer.answer(invocation);
			}
			return null;
		}

	}

	protected static enum MessageSendAnswer implements Answer<Void> {
		NoOp() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				// doNothing
				return null;
			}

		}, //

		NotAccept() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				SendMessageCommand sendMessageCmd = invocation.getArgumentAt(1, SendMessageCommand.class);
				CommandProcessor commandProcessor = PlexusComponentLocator.lookup(CommandProcessor.class,
				      CommandType.ACK_MESSAGE_SEND.toString());

				SendMessageAckCommand acceptCmd = new SendMessageAckCommand();
				acceptCmd.setSuccess(false);
				acceptCmd.correlate(sendMessageCmd);

				commandProcessor.process(new CommandProcessorContext(acceptCmd, null));

				return null;
			}
		}, //

		Accept() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				SendMessageCommand sendMessageCmd = invocation.getArgumentAt(1, SendMessageCommand.class);
				CommandProcessor commandProcessor = PlexusComponentLocator.lookup(CommandProcessor.class,
				      CommandType.ACK_MESSAGE_SEND.toString());

				SendMessageAckCommand acceptCmd = new SendMessageAckCommand();
				acceptCmd.setSuccess(true);
				acceptCmd.correlate(sendMessageCmd);

				commandProcessor.process(new CommandProcessorContext(acceptCmd, null));

				return null;
			}
		}, //

		ResponseSucessResult() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				SendMessageCommand sendMessageCmd = invocation.getArgumentAt(1, SendMessageCommand.class);
				CommandProcessor commandProcessor = PlexusComponentLocator.lookup(CommandProcessor.class,
				      CommandType.RESULT_MESSAGE_SEND.toString());

				SendMessageResultCommand resultCmd = new SendMessageResultCommand(sendMessageCmd.getMessageCount());
				resultCmd.correlate(sendMessageCmd);

				Map<Integer, Boolean> results = new HashMap<Integer, Boolean>();
				for (List<ProducerMessage<?>> pmsgList : sendMessageCmd.getProducerMessages()) {
					for (ProducerMessage<?> pmsg : pmsgList) {
						results.put(pmsg.getMsgSeqNo(), true);
					}
				}
				resultCmd.addResults(results);

				commandProcessor.process(new CommandProcessorContext(resultCmd, null));

				return null;
			}
		}, //

		ResponseFailResult() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				SendMessageCommand sendMessageCmd = invocation.getArgumentAt(1, SendMessageCommand.class);
				CommandProcessor commandProcessor = PlexusComponentLocator.lookup(CommandProcessor.class,
				      CommandType.RESULT_MESSAGE_SEND.toString());

				SendMessageResultCommand resultCmd = new SendMessageResultCommand(sendMessageCmd.getMessageCount());
				resultCmd.correlate(sendMessageCmd);

				Map<Integer, Boolean> results = new HashMap<Integer, Boolean>();
				for (List<ProducerMessage<?>> pmsgList : sendMessageCmd.getProducerMessages()) {
					for (ProducerMessage<?> pmsg : pmsgList) {
						results.put(pmsg.getMsgSeqNo(), false);
					}
				}
				resultCmd.addResults(results);

				commandProcessor.process(new CommandProcessorContext(resultCmd, null));

				return null;
			}
		};
	}

	public static class TestProducerConfig extends ProducerConfig {
		@Override
		public int getBrokerSenderTaskQueueSize() {
			return 1;
		}

		@Override
		public long getSendMessageReadResultTimeoutMillis() {
			return 100L;
		}

		@Override
		public long getBrokerSenderSendTimeoutMillis() {
			return 100L;
		}

		@Override
		public int getBrokerSenderNetworkIoCheckIntervalMaxMillis() {
			return 5;
		}

		@Override
		public int getBrokerSenderNetworkIoCheckIntervalBaseMillis() {
			return 1;
		}
	}

	public static class TestSendMessageResultMonitor extends DefaultSendMessageResultMonitor {
	}

	public static class TestMessageSender extends BrokerMessageSender {
		public void setEndpointClient(EndpointClient endpointClient) {
			m_endpointClient = endpointClient;
		}
	}

	public static class TestMetaService extends DefaultMetaService {

		private TestMetaHolder m_metaHolder;

		private MetaProxy m_metaProxy = new TestMetaProxy();

		public void setMetaHolder(TestMetaHolder metaHolder) {
			m_metaHolder = metaHolder;
		}

		public void setMetaProxy(MetaProxy metaProxy) {
			m_metaProxy = metaProxy;
		}

		@Override
		protected Meta getMeta() {
			return m_metaHolder.getMeta();
		}

		@Override
		protected MetaProxy getMetaProxy() {
			return m_metaProxy;
		}

		@Override
		public void initialize() throws InitializationException {
			// do nothing
		}

	}

	public static interface TestMetaHolder {
		public Meta getMeta();
	}

	public static class TestMetaProxy implements MetaProxy {

		private AtomicLong m_leaseId = new AtomicLong(0);

		@Override
		public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

		@Override
		public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + 10 * 1000L);
		}

		@Override
		public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
		      int brokerPort) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

		@Override
		public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
			long expireTime = System.currentTimeMillis() + 10 * 1000L;
			long leaseId = m_leaseId.incrementAndGet();
			return new LeaseAcquireResponse(true, new Lease(leaseId, expireTime), expireTime);
		}

		@Override
		public List<SchemaView> listSchemas() {
			return new ArrayList<SchemaView>();
		}

		@Override
		public List<SubscriptionView> listSubscriptions(String status) {
			return new ArrayList<SubscriptionView>();
		}

		@Override
		public int registerSchema(String schema, String subject) {
			return -1;
		}

		@Override
		public String getSchemaString(int schemaId) {
			return null;
		}

	}

}