package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.Guard;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventHandler.class, value = "LeaderInitEventHandler")
public class LeaderInitEventHandler extends BaseEventHandler implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(LeaderInitEventHandler.class);

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaService m_metaService;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Inject
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Inject
	private EndpointMaker m_endpointMaker;

	@Inject
	private Guard m_guard;

	private ServiceDiscovery<Void> m_serviceDiscovery;

	private ServiceCache<Void> m_serviceCache;

	public void setMetaService(MetaService metaService) {
		m_metaService = metaService;
	}

	public void setMetaHolder(MetaHolder metaHolder) {
		m_metaHolder = metaHolder;
	}

	public void setBrokerAssignmentHolder(BrokerAssignmentHolder brokerAssignmentHolder) {
		m_brokerAssignmentHolder = brokerAssignmentHolder;
	}

	public void setMetaServerAssignmentHolder(MetaServerAssignmentHolder metaServerAssignmentHolder) {
		m_metaServerAssignmentHolder = metaServerAssignmentHolder;
	}

	public void setEndpointMaker(EndpointMaker endpointMaker) {
		m_endpointMaker = endpointMaker;
	}

	@Override
	public EventType eventType() {
		return EventType.LEADER_INIT;
	}

	@Override
	public void initialize() throws InitializationException {
		m_serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class)//
		      .client(m_zkClient.get())//
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
			throw new InitializationException("Failed to start broker discovery service", e);
		}

	}

	@Override
	protected void processEvent(Event event) throws Exception {
		loadAndAddBaseMetaWatcher(new BaseMetaWatcher(event.getEventBus(), event.getStateHolder(), event.getVersion()));
		Meta baseMeta = loadBaseMeta();

		List<Server> metaServers = loadAndAddMetaServerListWatcher(new MetaServerListWatcher(event.getEventBus(),
		      event.getStateHolder(), event.getVersion()));

		Map<String, ClientContext> brokers = loadAndAddBrokerListWatcher(new BrokerChangedListener(event.getEventBus(),
		      event.getStateHolder(), event.getVersion()));
		ArrayList<Topic> topics = new ArrayList<>(baseMeta.getTopics().values());

		m_brokerAssignmentHolder.reload();
		m_brokerAssignmentHolder.reassign(brokers, topics);

		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoint = m_endpointMaker.makeEndpoints(event.getEventBus(),
		      event.getVersion(), event.getStateHolder(), m_brokerAssignmentHolder.getAssignments());

		m_metaHolder.setBaseMeta(baseMeta);
		m_metaHolder.setMetaServers(metaServers);
		m_metaHolder.update(topicPartition2Endpoint);

		m_metaServerAssignmentHolder.reload();
		m_metaServerAssignmentHolder.reassign(metaServers, topics);
	}

	private Map<String, ClientContext> loadAndAddBrokerListWatcher(ServiceCacheListener listener) {
		if (listener != null) {
			m_serviceCache.addListener(listener);
		}
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

	private List<Server> loadAndAddMetaServerListWatcher(Watcher watcher) throws Exception {
		List<Server> metaServers = new ArrayList<>();

		CuratorFramework client = m_zkClient.get();

		String rootPath = ZKPathUtils.getMetaServersZkPath();

		List<String> serverPaths = client.getChildren().usingWatcher(watcher).forPath(rootPath);

		for (String serverPath : serverPaths) {
			HostPort hostPort = ZKSerializeUtils.deserialize(
			      client.getData().forPath(ZKPaths.makePath(rootPath, serverPath)), HostPort.class);

			Server s = new Server();
			s.setHost(hostPort.getHost());
			s.setId(serverPath);
			s.setPort(hostPort.getPort());

			metaServers.add(s);
		}

		return metaServers;
	}

	private Long loadAndAddBaseMetaWatcher(Watcher watcher) throws Exception {
		byte[] data = m_zkClient.get().getData().usingWatcher(watcher).forPath(ZKPathUtils.getBaseMetaVersionZkPath());
		return ZKSerializeUtils.deserialize(data, Long.class);
	}

	private Meta loadBaseMeta() throws Exception {
		return m_metaService.findLatestMeta();
	}

	@Override
	protected Role role() {
		return Role.LEADER;
	}

	private class BaseMetaWatcher extends BaseEventBasedZkWatcher {

		private ClusterStateHolder m_clusterStateHolder;

		protected BaseMetaWatcher(EventBus eventBus, ClusterStateHolder clusterStateHolder, long version) {
			super(eventBus, version, org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
			m_clusterStateHolder = clusterStateHolder;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				Long baseMetaVersion = loadAndAddBaseMetaWatcher(this);

				m_eventBus.pubEvent(new com.ctrip.hermes.metaserver.event.Event(EventType.BASE_META_CHANGED, m_version,
				      m_clusterStateHolder, baseMetaVersion));
			} catch (Exception e) {
				log.error("Exception occurred while handling base meta watcher event.", e);
			}
		}

	}

	private class MetaServerListWatcher extends BaseEventBasedZkWatcher {

		private ClusterStateHolder m_clusterStateHolder;

		protected MetaServerListWatcher(EventBus eventBus, ClusterStateHolder clusterStateHolder, long version) {
			super(eventBus, version, org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged);
			m_clusterStateHolder = clusterStateHolder;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				List<Server> metaServers = loadAndAddMetaServerListWatcher(this);

				m_eventBus.pubEvent(new com.ctrip.hermes.metaserver.event.Event(EventType.META_SERVER_LIST_CHANGED,
				      m_version, m_clusterStateHolder, metaServers));
			} catch (Exception e) {
				log.error("Exception occurred while handling meta server list watcher event.", e);
			}
		}

	}

	private class BrokerChangedListener implements ServiceCacheListener {

		private EventBus m_eventBus;

		private ClusterStateHolder m_clusterStateHolder;

		private long m_version;

		public BrokerChangedListener(EventBus eventBus, ClusterStateHolder clusterStateHolder, long version) {
			m_eventBus = eventBus;
			m_clusterStateHolder = clusterStateHolder;
			m_version = version;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			// do nothing
		}

		@Override
		public void cacheChanged() {
			if (m_version == m_guard.getVersion()) {
				m_eventBus.getExecutor().submit(new Runnable() {

					@Override
					public void run() {
						Map<String, ClientContext> brokerList = loadAndAddBrokerListWatcher(null);
						m_eventBus.pubEvent(new Event(EventType.BROKER_LIST_CHANGED, m_version, m_clusterStateHolder,
						      brokerList));
					}
				});
			} else {
				m_serviceCache.removeListener(this);
			}
		}
	}

}
