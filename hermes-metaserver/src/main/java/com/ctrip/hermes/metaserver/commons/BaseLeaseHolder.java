package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
public abstract class BaseLeaseHolder<Key> implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(BaseLeaseHolder.class);

	@Inject
	protected SystemClockService m_systemClockService;

	@Inject
	protected ZookeeperService m_zookeeperService;

	@Inject
	protected ZKClient m_zkClient;

	private AtomicLong m_leaseIdGenerator = new AtomicLong(0);

	private Map<Key, LeaseContext> m_LeaseContexts = new HashMap<>();

	private ReentrantReadWriteLock m_LeaseContextsLock = new ReentrantReadWriteLock();

	public LeaseAcquireResponse executeLeaseOperation(Key contextKey, LeaseOperationCallback callback) throws Exception {

		LeaseContext leaseContext = getLeaseContext(contextKey);

		m_LeaseContextsLock.readLock().lock();
		try {
			try {
				leaseContext.lock();
				removeExpiredLeases(leaseContext.getExistingLeases());

				return callback.execute(leaseContext.getExistingLeases());
			} finally {
				leaseContext.unlock();
			}
		} finally {
			m_LeaseContextsLock.readLock().unlock();

		}
	}

	public Lease newLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      long leaseTimeMillis, String ip, int port) throws Exception {
		Lease newLease = new Lease(m_leaseIdGenerator.incrementAndGet(), m_systemClockService.now() + leaseTimeMillis);
		existingValidLeases.put(clientKey, new ClientLeaseInfo(newLease, ip, port));
		persistToZK(contextKey, existingValidLeases);
		return newLease;
	}

	public void renewLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      ClientLeaseInfo existingLeaseInfo, long leaseTimeMillis, String ip, int port) throws Exception {
		existingLeaseInfo.getLease().setExpireTime(existingLeaseInfo.getLease().getExpireTime() + leaseTimeMillis);
		existingLeaseInfo.setIp(ip);
		existingLeaseInfo.setPort(port);
		persistToZK(contextKey, existingValidLeases);
	}

	protected void persistToZK(Key contextKey, Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
		String path = convertKeyToZkPath(contextKey);
		String[] touchPaths = getZkPersistTouchPaths(contextKey);

		m_zookeeperService.persist(path, ZKSerializeUtils.serialize(existingValidLeases), touchPaths);
	}

	private void removeExpiredLeases(Map<String, ClientLeaseInfo> existingLeases) {
		Iterator<Entry<String, ClientLeaseInfo>> iterator = existingLeases.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, ClientLeaseInfo> entry = iterator.next();
			if (entry.getValue().getLease().isExpired()) {
				iterator.remove();
			}
		}
	}

	private LeaseContext getLeaseContext(Key contextKey) {
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

	public static interface LeaseOperationCallback {
		public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception;
	}

	private static class LeaseContext {
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

	public static class ClientLeaseInfo {
		private Lease m_lease;

		private AtomicReference<String> m_ip = new AtomicReference<>(null);

		private AtomicInteger m_port = new AtomicInteger();

		public ClientLeaseInfo() {
		}

		public ClientLeaseInfo(Lease lease, String ip, int port) {
			m_lease = lease;
			m_ip.set(ip);
			m_port.set(port);
		}

		public Lease getLease() {
			return m_lease;
		}

		public void setLease(Lease lease) {
			m_lease = lease;
		}

		public String getIp() {
			return m_ip.get();
		}

		public void setIp(String ip) {
			m_ip.set(ip);
		}

		public int getPort() {
			return m_port.get();
		}

		public void setPort(int port) {
			m_port.set(port);
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

	public void updateContexts(Map<String, Map<String, ClientLeaseInfo>> path2ExistingLeases) {
		for (Map.Entry<String, Map<String, ClientLeaseInfo>> entry : path2ExistingLeases.entrySet()) {
			updateLeaseContext(entry.getKey(), entry.getValue());
		}
	}

	protected abstract Map<String, Map<String, ClientLeaseInfo>> loadExistingLeases() throws Exception;

	protected Map<String, ClientLeaseInfo> deserializeExistingLeases(byte[] data) {
		return ZKSerializeUtils.deserialize(data, new TypeReference<Map<String, ClientLeaseInfo>>() {
		}.getType());
	}

	private void updateLeaseContext(String path, Map<String, ClientLeaseInfo> existingLeases) {
		if (existingLeases == null) {
			return;
		}

		Key contextKey = convertZkPathToKey(path);

		if (contextKey != null) {
			LeaseContext leaseContext = getLeaseContext(contextKey);

			m_LeaseContextsLock.readLock().lock();
			try {
				try {
					leaseContext.lock();
					leaseContext.setExistingLeases(existingLeases);

					m_LeaseContexts.put(contextKey, leaseContext);
				} finally {
					leaseContext.unlock();
				}
			} finally {
				m_LeaseContextsLock.readLock().unlock();

			}
		}
	}

	private void startHouseKeeper() {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("LeaseHolder-HouseKeeper", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      m_LeaseContextsLock.writeLock().lock();
				      try {
					      Iterator<Entry<Key, LeaseContext>> iterator = m_LeaseContexts.entrySet().iterator();
					      while (iterator.hasNext()) {
						      Entry<Key, LeaseContext> entry = iterator.next();
						      Map<String, ClientLeaseInfo> existingLeases = entry.getValue().getExistingLeases();
						      removeExpiredLeases(existingLeases);
						      if (existingLeases.isEmpty()) {
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

	protected abstract String[] getZkPersistTouchPaths(Key contextKey);

	protected abstract String convertKeyToZkPath(Key contextKey);

	protected abstract Key convertZkPathToKey(String path);

}
