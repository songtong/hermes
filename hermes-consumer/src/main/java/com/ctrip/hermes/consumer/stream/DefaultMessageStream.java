package com.ctrip.hermes.consumer.stream;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.ctrip.hermes.consumer.api.MessageStream;
import com.ctrip.hermes.consumer.api.MessageStreamOffset;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.monitor.PullMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.consumer.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch;
import com.ctrip.hermes.core.message.TppConsumerMessageBatch.MessageMeta;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.hermes.core.schedule.SchedulePolicy;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CorrelationIdGenerator;
import com.ctrip.hermes.core.transport.command.v2.AbstractPullMessageCommand;
import com.ctrip.hermes.core.transport.command.v2.PullSpecificMessageCommand;
import com.ctrip.hermes.core.transport.command.v3.PullMessageCommandV3;
import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.google.common.util.concurrent.SettableFuture;

public class DefaultMessageStream<T> implements MessageStream<T> {
	private static final Logger log = LoggerFactory.getLogger(DefaultMessageStream.class);

	private PullMessageResultMonitor m_pullMonitor = PlexusComponentLocator.lookup(PullMessageResultMonitor.class);

	private SystemClockService m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);

	private EndpointManager m_endpointManager = PlexusComponentLocator.lookup(EndpointManager.class);

	private EndpointClient m_endpointClient = PlexusComponentLocator.lookup(EndpointClient.class);

	private MessageCodec m_messageCodec = PlexusComponentLocator.lookup(MessageCodec.class);

	private ConsumerConfig m_config = PlexusComponentLocator.lookup(ConsumerConfig.class);

	private CoreConfig m_coreConfig = PlexusComponentLocator.lookup(CoreConfig.class);

	private final BlockingQueue<ConsumerMessage<T>> m_queue = new LinkedBlockingQueue<ConsumerMessage<T>>(1);

	private Iterator<ConsumerMessage<T>> m_iterator = new MessageStreamIterator<T>();

	private Class<?> m_messageClazz;

	private String m_topic;

	private String m_groupId;

	private int m_partitionId;

	public DefaultMessageStream(Tpg tpg, Class<?> messageClazz) {
		m_topic = tpg.getTopic();
		m_groupId = tpg.getGroupId();
		m_partitionId = tpg.getPartition();
		m_messageClazz = messageClazz;
	}

	@Override
	public int getParatitionId() {
		return m_partitionId;
	}

	@Override
	public String getTopic() {
		return m_topic;
	}

	@Override
	public Iterator<ConsumerMessage<T>> iterator() {
		return m_iterator;
	}

	public BlockingQueue<ConsumerMessage<T>> getQueue() {
		return m_queue;
	}

	@Override
	public boolean hasNext() {
		return !m_queue.isEmpty();
	}

	@Override
	public ConsumerMessage<T> next() {
		return m_iterator.next();
	}

	@SuppressWarnings("hiding")
	private class MessageStreamIterator<T> implements Iterator<ConsumerMessage<T>> {

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		@SuppressWarnings("unchecked")
		public ConsumerMessage<T> next() {
			try {
				return (ConsumerMessage<T>) m_queue.take();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log.error("Can not take message from queue.", e);
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Can not remove item from message stream.");
		}
	}

	protected List<ConsumerMessage<T>> decodeBatches(List<TppConsumerMessageBatch> batches) {
		List<ConsumerMessage<T>> msgs = new ArrayList<ConsumerMessage<T>>();
		if (batches != null && !batches.isEmpty()) {
			for (TppConsumerMessageBatch batch : batches) {
				List<MessageMeta> msgMetas = batch.getMessageMetas();
				ByteBuf batchData = batch.getData();

				int partition = batch.getPartition();

				for (int j = 0; j < msgMetas.size(); j++) {
					Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_topic, m_partitionId, m_groupId,
					      "decode-duration");
					Context context = timer.time();

					@SuppressWarnings("rawtypes")
					BaseConsumerMessage baseMsg = m_messageCodec.decode(batch.getTopic(), batchData, m_messageClazz);
					BrokerConsumerMessage<T> brokerMsg = new BrokerConsumerMessage<T>(baseMsg);
					MessageMeta messageMeta = msgMetas.get(j);
					brokerMsg.setPartition(partition);
					brokerMsg.setPriority(messageMeta.getPriority() == 0 ? true : false);
					brokerMsg.setResend(messageMeta.isResend());
					brokerMsg.setRetryTimesOfRetryPolicy(0);
					brokerMsg.setMsgSeq(messageMeta.getId());

					context.stop();

					msgs.add(brokerMsg);
				}
			}
		}
		return msgs;
	}

	private Command executePullMessageCommand(Endpoint endpoint, AbstractPullMessageCommand cmd, long timeout) {
		try {
			m_pullMonitor.monitor(cmd);
			m_endpointClient.writeCommand(endpoint, cmd);
			ConsumerStatusMonitor.INSTANCE.pullMessageCmdSent(m_topic, m_partitionId, m_groupId);
			return cmd.getFuture().get(timeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new RuntimeException("Pull specific message failed.", e);
		}
	}

	private static interface PullMessageCommandCreator {
		AbstractPullMessageCommand createPullCommand();
	}

	private List<ConsumerMessage<T>> pullMessages(PullMessageCommandCreator cmdCreator, long timeout) {
		Timer timer = ConsumerStatusMonitor.INSTANCE.getTimer(m_topic, m_partitionId, m_groupId, "pull-msg-cmd-duration");

		long expired = System.currentTimeMillis() + timeout;
		SchedulePolicy schedulePolicy = new ExponentialSchedulePolicy(//
		      (int) TimeUnit.SECONDS.toMillis(m_coreConfig.getMetaCacheRefreshIntervalSeconds()), (int) timeout);

		while (System.currentTimeMillis() < expired && !Thread.interrupted()) {
			Endpoint endpoint = m_endpointManager.getEndpoint(m_topic, m_partitionId);

			if (endpoint == null) {
				log.warn("No endpoint found for topic {} partition {}, will retry later", m_topic, m_partitionId);
			} else {
				AbstractPullMessageCommand cmd = cmdCreator.createPullCommand();
				Context context = timer.time();
				PullMessageResultCommandV3 ack = (PullMessageResultCommandV3) executePullMessageCommand(endpoint, cmd,
				      timeout);
				context.stop();

				if (ack == null) {
					log.warn("Wrong command result when fetch messages. {} {} {}", m_topic, m_partitionId, m_groupId);
					return new ArrayList<ConsumerMessage<T>>();
				}

				try {
					if (ack.isBrokerAccepted()) {
						return decodeBatches(ack.getBatches());
					}
				} finally {
					ack.release();
				}
			}
			schedulePolicy.fail(true);
		}

		log.warn("Fetch message timeout. {} {} {}", m_topic, m_partitionId, m_groupId);
		return null;
	}

	@Override
	public List<ConsumerMessage<T>> fetchMessages(final List<MessageStreamOffset> offsets) throws Exception {
		return pullMessages(new PullMessageCommandCreator() {
			@Override
			public AbstractPullMessageCommand createPullCommand() {
				SettableFuture<PullMessageResultCommandV3> future = SettableFuture.create();

				@SuppressWarnings("unchecked")
				List<Offset> cmdOffs = (List<Offset>) CollectionUtil.collect(offsets, new Transformer() {
					@Override
					public Object transform(Object offset) {
						MessageStreamOffset mOffset = (MessageStreamOffset) offset;
						return new Offset(mOffset.getPriorityOffset(), mOffset.getNonPriorityOffset(), null);
					}
				});

				long expireTime = m_systemClockService.now() + m_config.getPullMessageTimeoutMills();
				PullSpecificMessageCommand cmd = new PullSpecificMessageCommand(m_topic, m_partitionId, cmdOffs, expireTime);
				cmd.getHeader().setCorrelationId(CorrelationIdGenerator.generateCorrelationId());
				cmd.setFuture(future);

				return cmd;
			}
		}, m_config.getPullMessageTimeoutMills());
	}

	@Override
	public List<ConsumerMessage<T>> fetchMessages(final MessageStreamOffset offset, final int size) throws Exception {
		if (!(offset.getPriorityOffset() > 0) || !(offset.getNonPriorityOffset() > 0)) {
			throw new IllegalArgumentException("Offset must be a positive number ( >0 ).");
		}

		return pullMessages(new PullMessageCommandCreator() {
			@Override
			public AbstractPullMessageCommand createPullCommand() {
				SettableFuture<PullMessageResultCommandV3> future = SettableFuture.create();
				long expireTime = m_systemClockService.now() + m_config.getPullMessageTimeoutMills();
				Offset off = new Offset(offset.getPriorityOffset() - 1, offset.getNonPriorityOffset() - 1, null);
				PullMessageCommandV3 cmd = new PullMessageCommandV3(m_topic, m_partitionId, m_groupId, off, size,
				      expireTime);
				cmd.getHeader().setCorrelationId(CorrelationIdGenerator.generateCorrelationId());
				cmd.setFuture(future);
				return cmd;
			}
		}, m_config.getPullMessageTimeoutMills());
	}
}
