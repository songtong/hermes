package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaservice.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssigner.class)
public class DefaultBrokerAssigner implements BrokerAssigner, Initializable {

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_client;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private SystemClockService m_systemClockService;

	private ExecutorService m_executor;

	private ServiceDiscovery<Void> m_serviceDiscovery;

	private ServiceCache<Void> m_serviceCache;

	private AtomicReference<ServiceCacheListener> m_cacheListener = new AtomicReference<>(null);

	private AtomicInteger m_watcherVersion = new AtomicInteger(0);

	@Override
	public void stop(ClusterStateHolder stateHolder) {
		m_watcherVersion.incrementAndGet();
		ServiceCacheListener oldCacheListener = m_cacheListener.getAndSet(null);
		if (oldCacheListener != null) {
			m_serviceCache.removeListener(oldCacheListener);
		}
		m_brokerAssignmentHolder.stop();

	}

	@Override
	public void start(ClusterStateHolder stateHolder) {
		// submit to executor in case that any change event got notification before holder started
		m_executor.submit(new Runnable() {
			@Override
			public void run() {
				m_serviceCache.addListener(new BrokerChangedListener(m_watcherVersion.get()), m_executor);
				m_brokerAssignmentHolder.start();
			}
		});

	}

	private class BrokerChangedListener implements ServiceCacheListener {
		private int m_version;

		public BrokerChangedListener(int version) {
			m_version = version;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			// do nothing
		}

		@Override
		public void cacheChanged() {

			if (m_version != m_watcherVersion.get()) {
				return;
			}

			Map<String, ClientContext> brokers = getLatestBrokers();
			m_brokerAssignmentHolder.reassign(brokers);
		}

		private Map<String, ClientContext> getLatestBrokers() {
			List<ServiceInstance<Void>> instances = m_serviceCache.getInstances();
			Map<String, ClientContext> brokers = new HashMap<>();
			for (ServiceInstance<Void> instance : instances) {
				String name = instance.getId();
				String ip = instance.getAddress();
				int port = instance.getPort();
				brokers.put(name, new ClientContext(name, ip, port, m_systemClockService.now()));
			}
			return brokers;
		}
	}

	@Override
	public void initialize() throws InitializationException {
		m_serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class)//
		      .client(m_client.getClient())//
		      .basePath(m_config.getBrokerRegistryBasePath())//
		      .build();

		m_serviceCache = m_serviceDiscovery.serviceCacheBuilder()//
		      .name(m_config.getBrokerRegistryName(null))//
		      .threadFactory(HermesThreadFactory.create("brokerDiscoveryCache", true))//
		      .build();

		try {
			m_serviceDiscovery.start();
			m_serviceCache.start();
		} catch (Exception e) {
			throw new InitializationException("Failed to init broker assigner", e);
		}

		m_executor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("brokerDiscoveryCacheListener", true));
	}

}
