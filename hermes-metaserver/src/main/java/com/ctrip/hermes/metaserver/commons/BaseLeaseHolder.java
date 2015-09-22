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

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseLeaseHolder<Key> implements Initializable, LeaseHolder<Key> {

	private static final Logger log = LoggerFactory.getLogger(BaseLeaseHolder.class);

	@Inject
	protected SystemClockService m_systemClockService;

	@Inject
	protected ZookeeperService m_zookeeperService;

	@Inject
	protected ZKClient m_zkClient;

	private AtomicLong m_leaseIdGenerator = new AtomicLong(System.nanoTime());

	private Map<Key, LeaseContext> m_LeaseContexts = new HashMap<>();

	private ReentrantReadWriteLock m_LeaseContextsLock = new ReentrantReadWriteLock();

	@Override
	public Map<Key, Map<String, ClientLeaseInfo>> getAllValidLeases() throws Exception {
		Map<Key, Map<String, ClientLeaseInfo>> leases = new HashMap<>();

		clearExpiredLeases();

		m_LeaseContextsLock.readLock().lock();
		try {
			for (Map.Entry<Key, LeaseContext> entry : m_LeaseContexts.entrySet()) {
				Key key = entry.getKey();
				Map<String, ClientLeaseInfo> existingLeases = entry.getValue().getExistingLeases();
				if (existingLeases != null && !existingLeases.isEmpty()) {
					leases.put(key, new HashMap<String, ClientLeaseInfo>());

					for (Map.Entry<String, ClientLeaseInfo> existingLease : existingLeases.entrySet()) {
						leases.get(key)//
						      .put(existingLease.getKey(),//
						            new ClientLeaseInfo(existingLease.getValue().getLease(),
						                  existingLease.getValue().getIp(), existingLease.getValue().getPort()));
					}
				}

			}
		} finally {
			m_LeaseContextsLock.readLock().unlock();
		}

		return leases;
	}

	@Override
	public LeaseAcquireResponse executeLeaseOperation(Key contextKey, LeaseOperationCallback callback) throws Exception {

		LeaseContext leaseContext = getLeaseContext(contextKey);

		m_LeaseContextsLock.readLock().lock();
		try {
			leaseContext.lock();
			try {
				removeExpiredLeases(contextKey, leaseContext.getExistingLeases());

				return callback.execute(leaseContext.getExistingLeases());
			} finally {
				leaseContext.unlock();
			}
		} finally {
			m_LeaseContextsLock.readLock().unlock();

		}
	}

	@Override
	public Lease newLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      long leaseTimeMillis, String ip, int port) throws Exception {
		Lease newLease = new Lease(m_leaseIdGenerator.incrementAndGet(), m_systemClockService.now() + leaseTimeMillis);
		existingValidLeases.put(clientKey, new ClientLeaseInfo(newLease, ip, port));
		persistToZK(contextKey, existingValidLeases);
		return newLease;
	}

	@Override
	public void renewLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      ClientLeaseInfo existingLeaseInfo, long leaseTimeMillis, String ip, int port) throws Exception {
		existingValidLeases.put(clientKey, existingLeaseInfo);
		long newExpireTime = existingLeaseInfo.getLease().getExpireTime() + leaseTimeMillis;
		long now = m_systemClockService.now();
		if (newExpireTime > now + 2 * leaseTimeMillis) {
			newExpireTime = now + 2 * leaseTimeMillis;
		}
		existingLeaseInfo.getLease().setExpireTime(newExpireTime);
		existingLeaseInfo.setIp(ip);
		existingLeaseInfo.setPort(port);
		persistToZK(contextKey, existingValidLeases);
	}

	protected void persistToZK(Key contextKey, Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
		String path = convertKeyToZkPath(contextKey);
		String[] touchPaths = getZkPersistTouchPaths(contextKey);

		m_zookeeperService.persist(path, ZKSerializeUtils.serialize(existingValidLeases), touchPaths);
	}

	protected void removeExpiredLeases(Key contextKey, Map<String, ClientLeaseInfo> existingLeases) throws Exception {
		Iterator<Entry<String, ClientLeaseInfo>> iter = existingLeases.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, ClientLeaseInfo> entry = iter.next();
			Lease lease = entry.getValue().getLease();
			if (lease != null && lease.isExpired()) {
				iter.remove();
			}
		}
	}

	protected LeaseContext getLeaseContext(Key contextKey) {
		m_LeaseContextsLock.writeLock().lock();
		try {
			if (!m_LeaseContexts.containsKey(contextKey)) {
				m_LeaseContexts.put(contextKey, new LeaseContext());
			}

			return m_LeaseContexts.get(contextKey);
		} finally {
			m_LeaseContextsLock.writeLock().unlock();
		}

	}

	protected static class LeaseContext {
		private ReentrantLock m_lock = new ReentrantLock();

		private Map<String, ClientLeaseInfo> m_existingLeases = new HashMap<>();

		public void lock() {
			m_lock.lock();
		}

		public void unlock() {
			m_lock.unlock();
		}

		public Map<String, ClientLeaseInfo> getExistingLeases() {
			return m_existingLeases;
		}

		public void setExistingLeases(Map<String, ClientLeaseInfo> existingLeases) {
			m_existingLeases = existingLeases;
		}

	}

	@Override
	public void initialize() throws InitializationException {
		try {
			doInitialize();
			startHouseKeeper();
			loadAndWatchContexts();
		} catch (Exception e) {
			log.error("Failed to init LeaseHolder", e);
			throw new InitializationException("Failed to init LeaseHolder", e);
		}
	}

	protected void doInitialize() {

	}

	protected void loadAndWatchContexts() throws Exception {
		Map<String, Map<String, ClientLeaseInfo>> path2ExistingLeases = loadExistingLeases();

		updateContexts(path2ExistingLeases);
	}

	@Override
	public void updateContexts(Map<String, Map<String, ClientLeaseInfo>> path2ExistingLeases) throws Exception {
		for (Map.Entry<String, Map<String, ClientLeaseInfo>> entry : path2ExistingLeases.entrySet()) {
			updateLeaseContext(entry.getKey(), entry.getValue());
		}
	}

	protected abstract Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception;

	protected Map<String, ClientLeaseInfo> deserializeExistingLeases(byte[] data) {
		return ZKSerializeUtils.deserialize(data, new TypeReference<Map<String, ClientLeaseInfo>>() {
		}.getType());
	}

	private void updateLeaseContext(String path, Map<String, ClientLeaseInfo> existingLeases) throws Exception {
		if (existingLeases == null) {
			return;
		}

		Key contextKey = convertZkPathToKey(path);

		if (contextKey != null) {
			LeaseContext leaseContext = getLeaseContext(contextKey);

			m_LeaseContextsLock.readLock().lock();
			try {
				leaseContext.lock();
				try {
					leaseContext.setExistingLeases(existingLeases);
				} finally {
					leaseContext.unlock();
				}
			} finally {
				m_LeaseContextsLock.readLock().unlock();

			}
		}
	}

	protected void startHouseKeeper() {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("LeaseHolder-HouseKeeper", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      clearExpiredLeases();
			      }

		      }, 0, 5, TimeUnit.SECONDS);
	}

	protected void clearExpiredLeases() {
		m_LeaseContextsLock.writeLock().lock();
		try {
			Iterator<Entry<Key, LeaseContext>> iter = m_LeaseContexts.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<Key, LeaseContext> entry = iter.next();
				Map<String, ClientLeaseInfo> existingLeases = entry.getValue().getExistingLeases();
				removeExpiredLeases(entry.getKey(), existingLeases);
				if (existingLeases.isEmpty()) {
					iter.remove();
				}
			}
		} catch (Exception e) {
			log.warn("Exception occurred while doing housekeeping", e);
		} finally {
			m_LeaseContextsLock.writeLock().unlock();
		}
	}

	protected abstract String[] getZkPersistTouchPaths(Key contextKey);

	protected abstract String convertKeyToZkPath(Key contextKey);

	protected abstract Key convertZkPathToKey(String path);

}
