package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class BaseLeaseHolder<Key> implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(BaseLeaseHolder.class);

	@Inject
	private SystemClockService m_systemClockService;

	private AtomicLong m_leaseIdGenerator = new AtomicLong(0);

	private Map<Key, LeaseContext> m_LeaseContexts = new HashMap<>();

	private ReentrantReadWriteLock m_LeaseContextsLock = new ReentrantReadWriteLock();

	public LeaseAcquireResponse executeLeaseOperation(Key key, LeaseOperationCallback callback) {

		LeaseContext leaseContext = getLeaseContext(key);

		m_LeaseContextsLock.readLock().lock();
		try {
			try {
				leaseContext.lock();
				clearExpiredLeases(leaseContext.getExistingLeases());

				return callback.execute(leaseContext.getExistingLeases());
			} finally {
				leaseContext.unlock();
			}
		} finally {
			m_LeaseContextsLock.readLock().unlock();

		}
	}

	public Lease newLease(long leaseTimeMillis) {
		return new DefaultLease(m_leaseIdGenerator.incrementAndGet(), m_systemClockService.now() + leaseTimeMillis);
	}

	public static interface LeaseOperationCallback {
		public LeaseAcquireResponse execute(Map<String, Lease> existingValidLeases);
	}

	private void clearExpiredLeases(Map<String, Lease> existingLeases) {
		Iterator<Entry<String, Lease>> iterator = existingLeases.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Lease> entry = iterator.next();
			if (entry.getValue().isExpired()) {
				iterator.remove();
			}
		}
	}

	private LeaseContext getLeaseContext(Key key) {
		m_LeaseContextsLock.writeLock().lock();
		try {
			if (!m_LeaseContexts.containsKey(key)) {
				m_LeaseContexts.put(key, new LeaseContext());
			}

			return m_LeaseContexts.get(key);
		} finally {
			m_LeaseContextsLock.writeLock().unlock();
		}

	}

	private static class LeaseContext {
		private ReentrantLock m_lock = new ReentrantLock();

		private Map<String, Lease> m_existingLeases = new HashMap<>();

		public void lock() {
			m_lock.lock();
		}

		public void unlock() {
			m_lock.unlock();
		}

		public Map<String, Lease> getExistingLeases() {
			return m_existingLeases;
		}

	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("LeaseHolder-HouseKeeper", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      m_LeaseContextsLock.writeLock().lock();
				      try {
					      Iterator<Entry<Key, LeaseContext>> iterator = m_LeaseContexts.entrySet().iterator();
					      while (iterator.hasNext()) {
						      Entry<Key, LeaseContext> entry = iterator.next();
						      if (entry.getValue().getExistingLeases().isEmpty()) {
							      iterator.remove();
						      }
					      }
				      } catch (Exception e) {
					      log.warn("Exception occured while doing housekeeping", e);
				      } finally {
					      m_LeaseContextsLock.writeLock().unlock();
				      }
			      }
		      }, 0, 30, TimeUnit.SECONDS);
	}
}
