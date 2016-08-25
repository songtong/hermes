package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.log.LoggerConstants;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseLeaseHolder<Key> implements Initializable, LeaseHolder<Key> {

	private static final Logger log = LoggerFactory.getLogger(BaseLeaseHolder.class);

	private static final Logger traceLog = LoggerFactory.getLogger(LoggerConstants.TRACE);

	@Inject
	protected ZookeeperService m_zookeeperService;

	@Inject
	private LeaseHolderZkClient m_zkClient;

	@Inject
	private MetaServerConfig m_config;

	private AtomicLong m_leaseIdGenerator = new AtomicLong(System.nanoTime());

	protected AtomicBoolean m_inited = new AtomicBoolean(false);

	protected TreeCache m_treeCache;

	@Override
	public Map<Key, Map<String, ClientLeaseInfo>> getAllValidLeases() throws Exception {
		Map<Key, Map<String, ClientLeaseInfo>> leases = new HashMap<>();
		getValidLeasesRecursively(getBaseZkPath(), leases);
		return leases;
	}

	private void getValidLeasesRecursively(String path, Map<Key, Map<String, ClientLeaseInfo>> leases) {
		try {
			Map<String, ChildData> children = m_treeCache.getCurrentChildren(path);
			if (children != null && !children.isEmpty()) {
				for (ChildData child : children.values()) {
					if (isPathMatch(child.getPath()) && child.getData() != null && child.getData().length > 0) {
						Map<String, ClientLeaseInfo> existingLeases = deserializeExistingLeases(child.getData());
						removeExpiredLeases(existingLeases);
						if (!existingLeases.isEmpty()) {
							leases.put(convertZkPathToKey(child.getPath()), existingLeases);
						}
					} else {
						getValidLeasesRecursively(child.getPath(), leases);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred while getting valid leases recursively for path {}.", path, e);
		}
	}

	private Pair<Map<String, ClientLeaseInfo>, Integer> getValidLeaseAndVersion(String path) {
		try {
			ChildData child = m_treeCache.getCurrentData(path);

			if (child != null && child.getData() != null && child.getData().length > 0) {
				Map<String, ClientLeaseInfo> existingLeases = deserializeExistingLeases(child.getData());
				removeExpiredLeases(existingLeases);
				return new Pair<>(existingLeases, child.getStat().getVersion());
			}
		} catch (Exception e) {
			log.error("Exception occurred while getting valid leases for path {}.", path, e);
		}
		return null;
	}

	@Override
	public LeaseAcquireResponse executeLeaseOperation(Key contextKey, LeaseOperationCallback callback) throws Exception {
		Pair<Map<String, ClientLeaseInfo>, Integer> existingLeaseAndVersion = getValidLeaseAndVersion(convertKeyToZkPath(contextKey));
		if (existingLeaseAndVersion != null) {
			return callback.execute(existingLeaseAndVersion.getKey(), existingLeaseAndVersion.getValue());
		} else {
			return new LeaseAcquireResponse(false, null, System.currentTimeMillis()
			      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
		}
	}

	@Override
	public Lease newLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      int version, long leaseTimeMillis, String ip, int port) throws Exception {
		Lease newLease = new Lease(m_leaseIdGenerator.incrementAndGet(), System.currentTimeMillis() + leaseTimeMillis);
		existingValidLeases.put(clientKey, new ClientLeaseInfo(newLease, ip, port));

		if (persistToZK(contextKey, existingValidLeases, version)) {
			return newLease;
		} else {
			return null;
		}
	}

	@Override
	public boolean renewLease(Key contextKey, String clientKey, Map<String, ClientLeaseInfo> existingValidLeases,
	      ClientLeaseInfo existingLeaseInfo, int version, long leaseTimeMillis, String ip, int port) throws Exception {

		long newExpireTime = existingLeaseInfo.getLease().getExpireTime() + leaseTimeMillis;
		long now = System.currentTimeMillis();
		if (newExpireTime > now + 2 * leaseTimeMillis) {
			newExpireTime = now + 2 * leaseTimeMillis;
		}

		existingLeaseInfo.getLease().setExpireTime(newExpireTime);
		existingLeaseInfo.setIp(ip);
		existingLeaseInfo.setPort(port);
		existingValidLeases.put(clientKey, existingLeaseInfo);

		return persistToZK(contextKey, existingValidLeases, version);
	}

	protected boolean persistToZK(Key contextKey, Map<String, ClientLeaseInfo> existingValidLeases, int version)
	      throws Exception {
		String path = convertKeyToZkPath(contextKey);
		return m_zookeeperService.persistWithVersionCheck(path, ZKSerializeUtils.serialize(existingValidLeases), version);
	}

	protected void removeExpiredLeases(Map<String, ClientLeaseInfo> existingLeases) {
		if (existingLeases != null) {
			Iterator<Entry<String, ClientLeaseInfo>> iter = existingLeases.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, ClientLeaseInfo> entry = iter.next();
				Lease lease = entry.getValue().getLease();
				if (lease != null && lease.isExpired()) {
					iter.remove();
				}
			}
		}
	}

	@Override
	public boolean inited() {
		return m_inited.get();
	}

	@Override
	public void initialize() throws InitializationException {
		log.info("Start to init {}", this.getClass().getSimpleName());
		try {
			doInitialize();
			startDetailPrinterThread();
			m_inited.set(true);
			log.info("{} inited", getName());
		} catch (Exception e) {
			log.error("Failed to init LeaseHolder", e);
			throw new InitializationException("Failed to init LeaseHolder", e);
		}
	}

	private void startDetailPrinterThread() {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(getName() + "-DetailPrinter", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      Map<Key, Map<String, ClientLeaseInfo>> allValidLeases = getAllValidLeases();
					      if (traceLog.isInfoEnabled()) {
						      traceLog.info(getName() + "\n" + JSON.toJSONString(allValidLeases));
					      }
				      } catch (Exception e) {
					      // ignore
				      }
			      }

		      }, 1, 1, TimeUnit.MINUTES);

	}

	protected void doInitialize() throws Exception {
		m_treeCache = new TreeCache(m_zkClient.get(), getBaseZkPath());
		m_treeCache.start();
	}

	protected Map<String, ClientLeaseInfo> deserializeExistingLeases(byte[] data) {
		return ZKSerializeUtils.deserialize(data, new TypeReference<Map<String, ClientLeaseInfo>>() {
		}.getType());
	}

	protected abstract String getName();

	protected abstract String convertKeyToZkPath(Key contextKey);

	protected abstract Key convertZkPathToKey(String path);

	protected abstract String getBaseZkPath();

	protected abstract boolean isPathMatch(String path);

}
