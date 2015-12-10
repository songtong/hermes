package com.ctrip.hermes.consumer.engine.ack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Timer.Context;
import com.ctrip.hermes.consumer.engine.ack.AckHolder.AckCallback;
import com.ctrip.hermes.consumer.engine.ack.AckHolder.NackCallback;
import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.engine.monitor.AckMessageResultMonitor;
import com.ctrip.hermes.consumer.engine.status.ConsumerStatusMonitor;
import com.ctrip.hermes.core.bo.AckContext;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.BaseConsumerMessageAware;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.transport.command.v3.AckMessageCommandV3;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.endpoint.EndpointManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.dianping.cat.Cat;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = AckManager.class)
public class DefaultAckManager implements AckManager {

	private static final Logger log = LoggerFactory.getLogger(DefaultAckManager.class);

	@Inject
	private EndpointManager m_endpointManager;

	@Inject
	private EndpointClient m_endpointClient;

	@Inject
	private ConsumerConfig m_config;

	@Inject
	private AckMessageResultMonitor m_resultMonitor;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	private ExecutorService m_ackCheckerIoExecutor;

	private ConcurrentMap<Long, TpgAckHolder> m_ackHolders = new ConcurrentHashMap<>();

	@Override
	public void register(long correlationId, Tpg tpg, int maxAckHolderSize) {
		if (m_started.compareAndSet(false, true)) {
			startChecker();
		}

		m_ackHolders.putIfAbsent(correlationId, new TpgAckHolder(tpg, correlationId, maxAckHolderSize));
	}

	@Override
	public void ack(long correlationId, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), correlationId);
		if (holder != null) {
			BaseConsumerMessage<?> bcm = msg instanceof BaseConsumerMessageAware ? ((BaseConsumerMessageAware<?>) msg)
			      .getBaseConsumerMessage() : null;

			long onMessageStart = bcm == null ? -1L : bcm.getOnMessageStartTimeMills();
			long onMessageEnd = bcm == null ? -1L : bcm.getOnMessageEndTimeMills();
			holder.offerOperation(new AckOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), onMessageStart,
			      onMessageEnd));
		}
	}

	@Override
	public void nack(long correlationId, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), correlationId);
		if (holder != null) {
			BaseConsumerMessage<?> bcm = msg instanceof BaseConsumerMessageAware ? ((BaseConsumerMessageAware<?>) msg)
			      .getBaseConsumerMessage() : null;

			long onMessageStart = bcm == null ? -1L : bcm.getOnMessageStartTimeMills();
			long onMessageEnd = bcm == null ? -1L : bcm.getOnMessageEndTimeMills();
			holder.offerOperation(new NackOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), onMessageStart,
			      onMessageEnd));
		}
	}

	@Override
	public void delivered(long correlationId, ConsumerMessage<?> msg) {
		TpgAckHolder holder = findTpgAckHolder(msg.getTopic(), msg.getPartition(), correlationId);
		if (holder != null) {
			holder.offerOperation(new DeliveredOperation(msg.getOffset(), msg.isPriority(), msg.isResend(), msg
			      .getRemainingRetries()));
		}
	}

	private TpgAckHolder findTpgAckHolder(String topic, int partition, long correlationId) {
		TpgAckHolder holder = m_ackHolders.get(correlationId);
		if (holder == null) {
			log.warn("TpgAckHolder not found(topic={}, partition={}).", topic, partition);
			return null;
		} else {
			return holder;
		}
	}

	@Override
	public void deregister(long correlationId) {
		TpgAckHolder holder = m_ackHolders.get(correlationId);

		if (holder != null) {
			holder.stop();

			while (!Thread.interrupted()) {
				if (holder.hasUnhandleOperation()) {
					try {
						TimeUnit.MILLISECONDS.sleep(50);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	private void startChecker() {
		m_ackCheckerIoExecutor = Executors.newFixedThreadPool(m_config.getAckCheckerIoThreadCount(),
		      HermesThreadFactory.create("AckCheckerIo", true));

		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("AckChecker", true))
		      .scheduleWithFixedDelay(new AckHolderChecker(), m_config.getAckCheckerIntervalMillis(),
		            m_config.getAckCheckerIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	private class AckHolderChecker implements Runnable {

		public void run() {
			List<TpgAckHolder> holders = new ArrayList<>(m_ackHolders.values());
			Collections.shuffle(holders);

			for (TpgAckHolder holder : holders) {
				try {
					holder.handleOperations();
					check(holder);
				} catch (Exception e) {
					log.error("Exception occurred while executing AckChecker task.", e);
				}
			}
		}

		private void check(final TpgAckHolder holder) {
			if (holder.startFlush()) {
				m_ackCheckerIoExecutor.submit(new HolderFlushTask(holder));
			}
		}

		private class HolderFlushTask implements Runnable {

			private TpgAckHolder m_holder;

			public HolderFlushTask(TpgAckHolder holder) {
				m_holder = holder;
			}

			@Override
			public void run() {
				Tpg tpg = m_holder.getTpg();
				AckMessageCommandV3 cmd = m_holder.pop();
				try {
					if (cmd != null) {
						Endpoint endpoint = m_endpointManager.getEndpoint(cmd.getTopic(), cmd.getPartition());

						if (endpoint != null) {
							long correlationId = cmd.getHeader().getCorrelationId();
							Future<Boolean> resultFuture = m_resultMonitor.monitor(correlationId);

							int timeout = m_config.getAckCheckerIoTimeoutMillis();

							Context ackedTimer = ConsumerStatusMonitor.INSTANCE.getTimer(tpg.getTopic(), tpg.getPartition(),
							      tpg.getGroupId(), "ack-msg-cmd-duration").time();

							m_endpointClient.writeCommand(endpoint, cmd, timeout, TimeUnit.MILLISECONDS);
							ConsumerStatusMonitor.INSTANCE.ackMessageCmdSent(tpg.getTopic(), tpg.getPartition(),
							      tpg.getGroupId());

							try {
								Boolean acked = resultFuture.get(timeout, TimeUnit.MILLISECONDS);

								if (acked != null && acked) {
									ConsumerStatusMonitor.INSTANCE.brokerAcked(tpg.getTopic(), tpg.getPartition(),
									      tpg.getGroupId());
									cmd = null;
								} else {
									ConsumerStatusMonitor.INSTANCE.brokerAckFailed(tpg.getTopic(), tpg.getPartition(),
									      tpg.getGroupId());
								}
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
								m_resultMonitor.cancel(correlationId);
							} catch (TimeoutException e) {
								ConsumerStatusMonitor.INSTANCE.waitBrokerAckMessageTimeout(tpg.getTopic(), tpg.getPartition(),
								      tpg.getGroupId());
								m_resultMonitor.cancel(correlationId);
							} catch (Exception e) {
								ConsumerStatusMonitor.INSTANCE.brokerAckFailed(tpg.getTopic(), tpg.getPartition(),
								      tpg.getGroupId());
								m_resultMonitor.cancel(correlationId);
							}

							ackedTimer.stop();

						} else {
							log.debug("No endpoint found, ignore it");
						}
					} else {
						if (m_holder.isStopped()) {
							m_ackHolders.remove(m_holder.getCorrelationId());
						}
					}
				} catch (Exception e) {
					ConsumerStatusMonitor.INSTANCE.brokerAckFailed(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
					log.warn("Exception occurred while flushing ack cmd to broker, will retry it", e);
				} finally {
					if (cmd != null) {
						m_holder.push(cmd);
					}

					m_holder.finishFlush();
				}
			}
		}
	}

	private class TpgAckHolder {
		private Tpg m_tpg;

		private int m_maxAckHolderSize;

		private long m_correlationId;

		private AckHolder<AckContext> m_priorityAckHolder;

		private AckHolder<AckContext> m_nonpriorityAckHolder;

		private AckHolder<AckContext> m_resendAckHolder;

		private BlockingQueue<Operation> m_opQueue;

		private AtomicBoolean m_stopped = new AtomicBoolean(false);

		private AtomicReference<AckMessageCommandV3> m_cmd = new AtomicReference<>(null);

		private AtomicBoolean m_flushing = new AtomicBoolean(false);

		public TpgAckHolder(Tpg tpg, long correlationId, int maxAckHolderSize) {
			m_tpg = tpg;
			m_correlationId = correlationId;
			m_maxAckHolderSize = maxAckHolderSize;

			m_priorityAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_nonpriorityAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_resendAckHolder = new DefaultAckHolder<>(m_tpg, m_maxAckHolderSize);
			m_opQueue = new LinkedBlockingQueue<>();
		}

		public void finishFlush() {
			m_flushing.set(false);
		}

		public boolean startFlush() {
			return m_flushing.compareAndSet(false, true);
		}

		public void stop() {
			m_stopped.set(true);
		}

		public AckMessageCommandV3 pop() {
			if (m_cmd.get() == null) {
				return scan();
			}
			return m_cmd.getAndSet(null);
		}

		public void push(AckMessageCommandV3 cmd) {
			m_cmd.set(cmd);
		}

		public Tpg getTpg() {
			return m_tpg;
		}

		private AckMessageCommandV3 scan() {
			AckMessageCommandV3 cmd = null;

			AckHolderScanningResult<AckContext> priorityScanningRes = m_priorityAckHolder.scan();
			AckHolderScanningResult<AckContext> nonpriorityScanningRes = m_nonpriorityAckHolder.scan();
			AckHolderScanningResult<AckContext> resendScanningRes = m_resendAckHolder.scan();

			if (!priorityScanningRes.getAcked().isEmpty() //
			      || !priorityScanningRes.getNacked().isEmpty() //
			      || !nonpriorityScanningRes.getAcked().isEmpty() //
			      || !nonpriorityScanningRes.getNacked().isEmpty() //
			      || !resendScanningRes.getAcked().isEmpty() //
			      || !resendScanningRes.getNacked().isEmpty()) {

				cmd = new AckMessageCommandV3(m_tpg.getTopic(), m_tpg.getPartition(), m_tpg.getGroupId());
				cmd.getHeader().setCorrelationId(m_correlationId);

				// priority
				for (AckContext ctx : priorityScanningRes.getAcked()) {
					cmd.addAckMsg(true, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : priorityScanningRes.getNacked()) {
					cmd.addNackMsg(true, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				// non-priority
				for (AckContext ctx : nonpriorityScanningRes.getAcked()) {
					cmd.addAckMsg(false, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : nonpriorityScanningRes.getNacked()) {
					cmd.addNackMsg(false, false, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				// resend
				for (AckContext ctx : resendScanningRes.getAcked()) {
					cmd.addAckMsg(false, true, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}
				for (AckContext ctx : resendScanningRes.getNacked()) {
					cmd.addNackMsg(false, true, ctx.getMsgSeq(), ctx.getRemainingRetries(),
					      ctx.getOnMessageStartTimeMillis(), ctx.getOnMessageEndTimeMillis());
				}

			}

			return cmd;
		}

		public long getCorrelationId() {
			return m_correlationId;
		}

		private AckHolder<AckContext> findHolder(Operation op) {
			if (op.isResend()) {
				return m_resendAckHolder;
			} else {
				return op.isPriority() ? m_priorityAckHolder : m_nonpriorityAckHolder;
			}
		}

		public void handleOperations() {
			List<Operation> todos = new ArrayList<Operation>();

			m_opQueue.drainTo(todos);

			if (!todos.isEmpty()) {
				for (Operation op : todos) {
					try {
						AckHolder<AckContext> holder = findHolder(op);
						if (op instanceof DeliveredOperation) {
							DeliveredOperation dOp = (DeliveredOperation) op;

							holder.delivered(dOp.getId(), new AckContext(dOp.getId(), dOp.getRemainingRetries(), -1, -1));
						} else if (op instanceof AckOperation) {
							final AckOperation aOp = (AckOperation) op;
							holder.ack(aOp.getId(), new AckCallback<AckContext>() {

								@Override
								public void doBeforeAck(AckContext item) {
									item.setOnMessageStartTimeMillis(aOp.getOnMessageStart());
									item.setOnMessageEndTimeMillis(aOp.getOnMessageEnd());
								}
							});
						} else if (op instanceof NackOperation) {
							final NackOperation aOp = (NackOperation) op;
							holder.nack(aOp.getId(), new NackCallback<AckContext>() {

								@Override
								public void doBeforeNack(AckContext item) {
									item.setOnMessageStartTimeMillis(aOp.getOnMessageStart());
									item.setOnMessageEndTimeMillis(aOp.getOnMessageEnd());
								}
							});
						}
					} catch (AckHolderException e) {
						log.error("Exception occurred while handling operations({}).", m_tpg, e);
						Cat.logError(e);
					}
				}
			}
		}

		public boolean isStopped() {
			return m_stopped.get();
		}

		public void offerOperation(Operation op) {
			if (!m_stopped.get()) {
				m_opQueue.offer(op);
			}
		}

		public boolean hasUnhandleOperation() {
			return !m_opQueue.isEmpty();
		}
	}

	static class Operation {
		private long m_id;

		private boolean m_priority;

		private boolean m_resend;

		public Operation(long id, boolean priority, boolean resend) {
			m_id = id;
			m_priority = priority;
			m_resend = resend;
		}

		public long getId() {
			return m_id;
		}

		public boolean isPriority() {
			return m_priority;
		}

		public boolean isResend() {
			return m_resend;
		}

	}

	static class DeliveredOperation extends Operation {

		private int m_remainingRetries;

		public DeliveredOperation(long id, boolean priority, boolean resend, int remainingRetries) {
			super(id, priority, resend);
			m_remainingRetries = remainingRetries;
		}

		public int getRemainingRetries() {
			return m_remainingRetries;
		}

	}

	static class NackOperation extends Operation {

		private long m_onMessageStart;

		private long m_onMessageEnd;

		public NackOperation(long id, boolean priority, boolean resend, long onMessageStart, long onMessageEnd) {
			super(id, priority, resend);
			m_onMessageStart = onMessageStart;
			m_onMessageEnd = onMessageEnd;
		}

		public long getOnMessageStart() {
			return m_onMessageStart;
		}

		public long getOnMessageEnd() {
			return m_onMessageEnd;
		}

	}

	static class AckOperation extends Operation {

		private long m_onMessageStart;

		private long m_onMessageEnd;

		public AckOperation(long id, boolean priority, boolean resend, long onMessageStart, long onMessageEnd) {
			super(id, priority, resend);
			m_onMessageStart = onMessageStart;
			m_onMessageEnd = onMessageEnd;
		}

		public long getOnMessageStart() {
			return m_onMessageStart;
		}

		public long getOnMessageEnd() {
			return m_onMessageEnd;
		}

	}

}