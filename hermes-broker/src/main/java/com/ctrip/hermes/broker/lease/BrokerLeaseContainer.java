package com.ctrip.hermes.broker.lease;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.broker.build.BuildConstants;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.lease.BrokerLeaseManager.BrokerLeaseKey;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.lease.LeaseManager;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseContainer.class)
public class BrokerLeaseContainer implements Initializable {

	@Inject(BuildConstants.BROKER)
	private LeaseManager<BrokerLeaseKey> m_leaseManager;

	@Inject
	private BrokerConfig m_config;

	@Inject
	private SystemClockService m_systemClockService;

	private Map<BrokerLeaseKey, Lease> m_existingLeases = new ConcurrentHashMap<>();

	private Map<Pair<String, Integer>, Long> m_nextAcquireTimes = new ConcurrentHashMap<>();

	private ConcurrentMap<BrokerLeaseKey, AtomicBoolean> m_leaseAcquireTaskRunnings = new ConcurrentHashMap<>();

	private ScheduledExecutorService m_scheduledExecutorService;

	public Lease acquireLease(String topic, int partition, String sessionId) {
		BrokerLeaseKey key = new BrokerLeaseKey(topic, partition, sessionId);
		Lease lease = m_existingLeases.get(key);
		if (lease == null || lease.isExpired()) {
			scheduleLeaseAcquireTask(key);
			return null;
		} else {
			return lease;
		}
	}

	private void scheduleLeaseAcquireTask(final BrokerLeaseKey key) {
		m_leaseAcquireTaskRunnings.putIfAbsent(key, new AtomicBoolean(false));

		final AtomicBoolean acquireTaskRunning = m_leaseAcquireTaskRunnings.get(key);
		if (acquireTaskRunning.compareAndSet(false, true)) {

			m_scheduledExecutorService.submit(new Runnable() {

				@Override
				public void run() {
					try {
						doAcquireLease(key);
					} finally {
						acquireTaskRunning.set(false);
					}
				}

			});
		}

	}

	private void doAcquireLease(BrokerLeaseKey key) {
		Lease existingLease = m_existingLeases.get(key);
		if (existingLease != null && !existingLease.isExpired()) {
			return;
		}

		Pair<String, Integer> tp = new Pair<String, Integer>(key.getTopic(), key.getPartition());

		if (m_nextAcquireTimes.containsKey(tp) && m_nextAcquireTimes.get(tp) > m_systemClockService.now()) {
			return;
		}

		LeaseAcquireResponse response = m_leaseManager.tryAcquireLease(key);

		if (response != null && response.isAcquired()) {
			Lease lease = response.getLease();
			if (!lease.isExpired()) {
				m_existingLeases.put(key, lease);

				long renewDelay = lease.getRemainingTime() - m_config.getLeaseRenewTimeMillsBeforeExpire();
				m_nextAcquireTimes.remove(tp);
				scheduleRenewLeaseTask(key, renewDelay);
			}
		} else {
			if (response != null) {
				m_nextAcquireTimes.put(tp, response.getNextTryTime());
			} else {
				m_nextAcquireTimes.put(tp, m_config.getDefaultLeaseAcquireDelay() + m_systemClockService.now());
			}
		}
	}

	private void scheduleRenewLeaseTask(final BrokerLeaseKey key, long delay) {
		if (delay < 0) {
			return;
		}

		m_scheduledExecutorService.schedule(new Runnable() {

			@Override
			public void run() {
				Lease existingLease = m_existingLeases.get(key);
				if (existingLease != null) {
					LeaseAcquireResponse response = m_leaseManager.tryRenewLease(key, existingLease);

					if (response != null && response.isAcquired()) {
						existingLease.setExpireTime(response.getLease().getExpireTime());
					} else {
						if (response != null && response.getNextTryTime() > 0) {
							scheduleRenewLeaseTask(key, response.getNextTryTime() - m_systemClockService.now());
						} else {
							scheduleRenewLeaseTask(key, m_config.getDefaultLeaseRenewDelay());
						}
					}
				}
			}
		}, delay, TimeUnit.MILLISECONDS);

	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutorService = Executors.newScheduledThreadPool(m_config.getLeaseContainerThreadCount(),
		      HermesThreadFactory.create("BrokerLeaseContainer", false));
	}
}
