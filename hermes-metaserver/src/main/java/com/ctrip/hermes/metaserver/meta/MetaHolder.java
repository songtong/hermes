package com.ctrip.hermes.metaserver.meta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaHolder.class)
public class MetaHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(MetaHolder.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private ZookeeperService m_zkService;

	private AtomicReference<Meta> m_mergedCache = new AtomicReference<>();

	private AtomicReference<Meta> m_baseCache = new AtomicReference<>();

	private AtomicReference<List<Server>> m_metaServerListCache = new AtomicReference<>();

	// topic -> partition -> endpoint
	private AtomicReference<Map<String, Map<Integer, Endpoint>>> m_endpointCache = new AtomicReference<>();

	private ExecutorService m_updateTaskExecutor;

	private MetaMerger m_metaMerger = new MetaMerger();

	public Meta getMeta() {
		return m_mergedCache.get();
	}

	@Override
	public void initialize() throws InitializationException {
		m_updateTaskExecutor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("MetaUpdater", true));
	}

	public void setMeta(Meta meta) {
		m_mergedCache.set(meta);
	}

	public void setMetaServers(List<Server> metaServers) {
		m_metaServerListCache.set(metaServers);
	}

	public void setBaseMeta(Meta baseMeta) {
		m_baseCache.set(baseMeta);
	}

	public void update(final Meta baseMeta) {
		update(baseMeta, null, null);
	}

	public void update() {
		update(null, null, null);
	}

	public void update(final List<Server> metaServerList) {
		update(null, metaServerList, null);
	}

	public void update(final Map<String, Map<Integer, Endpoint>> newEndpoints) {
		update(null, null, newEndpoints);
	}

	private void update(final Meta baseMeta, final List<Server> metaServerList,
	      final Map<String, Map<Integer, Endpoint>> newEndpoints) {
		if (baseMeta != null) {
			m_baseCache.set(baseMeta);
		}
		if (metaServerList != null) {
			m_metaServerListCache.set(metaServerList);
		}
		if (newEndpoints != null) {
			m_endpointCache.set(newEndpoints);
		}

		m_updateTaskExecutor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					Meta newMeta = m_metaMerger.merge(m_baseCache.get(), m_metaServerListCache.get(), m_endpointCache.get());
					upgradeMetaVersion(newMeta);
					m_mergedCache.set(newMeta);
				} catch (Exception e) {
					log.error("Exception occurred while updating meta.", e);
				}
			}

		});
	}

	private void upgradeMetaVersion(Meta meta) throws Exception {
		MetaInfo metaInfo = ZKSerializeUtils.deserialize(
		      m_zkClient.get().getData().forPath(ZKPathUtils.getMetaInfoZkPath()), MetaInfo.class);

		long newMetaVersion = System.currentTimeMillis() / 1000L;
		// may be same due to different machine time
		if (metaInfo != null) {
			newMetaVersion = metaInfo.getTimestamp() >= newMetaVersion ? metaInfo.getTimestamp() + 1 : newMetaVersion;
		}

		meta.setVersion(newMetaVersion);

		if (metaInfo == null) {
			metaInfo = new MetaInfo();
		}

		metaInfo.setTimestamp(newMetaVersion);
		metaInfo.setHost(Networks.forIp().getLocalHostAddress());
		metaInfo.setPort(m_config.getMetaServerPort());

		m_zkService.persist(ZKPathUtils.getMetaInfoZkPath(), ZKSerializeUtils.serialize(metaInfo));

		log.info("Upgrade dynamic meta(version={}).", metaInfo.getTimestamp());
	}

}
