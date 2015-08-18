package com.ctrip.hermes.metaserver.event.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.ctrip.hermes.metaserver.commons.BaseEventBasedZkWatcher;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.commons.EndpointMaker;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventEngineContext;
import com.ctrip.hermes.metaserver.event.EventHandler;
import com.ctrip.hermes.metaserver.event.EventType;
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

	private ServiceDiscovery<Void> m_serviceDiscovery;

	private ServiceCache<Void> m_serviceCache;

	private AtomicBoolean m_brokerListenerAdded = new AtomicBoolean(false);

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
	protected void processEvent(EventEngineContext context, Event event) throws Exception {
		// FIXME refine thread pool
		loadAndAddBaseMetaWatcher(new BaseMetaWatcher(context));
		Meta baseMeta = loadBaseMeta();

		List<Server> metaServers = loadAndAddMetaServerListWatcher(new MetaServerListWatcher(context));

		Map<String, ClientContext> brokers = loadAndAddBrokerListWatcher(new BrokerChangedListener(context));
		ArrayList<Topic> topics = new ArrayList<>(baseMeta.getTopics().values());

		m_brokerAssignmentHolder.reload();
		m_brokerAssignmentHolder.reassign(brokers, topics);

		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoint = m_endpointMaker.makeEndpoints(context,
		      m_brokerAssignmentHolder.getAssignments());

		m_metaHolder.setBaseMeta(baseMeta);
		m_metaHolder.setMetaServers(metaServers);
		m_metaHolder.update(topicPartition2Endpoint);

		m_metaServerAssignmentHolder.reload();
		m_metaServerAssignmentHolder.reassign(metaServers, topics);
	}

	private Map<String, ClientContext> loadAndAddBrokerListWatcher(ServiceCacheListener listener) {
		if (m_brokerListenerAdded.compareAndSet(false, true)) {
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

		private EventEngineContext m_context;

		protected BaseMetaWatcher(EventEngineContext context) {
			super(context.getEventBus(), context.getWatcherExecutor(), context.getClusterStateHolder(),
			      org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged);
			m_context = context;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				Long version = loadAndAddBaseMetaWatcher(this);

				// FIXME context.pubEvent
				m_eventBus.pubEvent(m_context, new com.ctrip.hermes.metaserver.event.Event(EventType.BASE_META_CHANGED,
				      version));
			} catch (Exception e) {
				log.error("Exception occurred while handling base meta watcher event.", e);
			}
		}

	}

	private class MetaServerListWatcher extends BaseEventBasedZkWatcher {

		private EventEngineContext m_context;

		protected MetaServerListWatcher(EventEngineContext context) {
			super(context.getEventBus(), context.getWatcherExecutor(), context.getClusterStateHolder(),
			      org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged);
			m_context = context;
		}

		@Override
		protected void doProcess(WatchedEvent event) {
			try {
				List<Server> metaServers = loadAndAddMetaServerListWatcher(this);

				m_eventBus.pubEvent(m_context, new com.ctrip.hermes.metaserver.event.Event(
				      EventType.META_SERVER_LIST_CHANGED, metaServers));
			} catch (Exception e) {
				log.error("Exception occurred while handling meta server list watcher event.", e);
			}
		}

	}

	private class BrokerChangedListener implements ServiceCacheListener {

		private EventEngineContext m_context;

		public BrokerChangedListener(EventEngineContext context) {
			m_context = context;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			// do nothing
		}

		@Override
		public void cacheChanged() {

			final EventBus eventBus = m_context.getEventBus();
			ExecutorService watcherExecutor = m_context.getWatcherExecutor();
			if (!watcherExecutor.isShutdown()) {
				watcherExecutor.submit(new Runnable() {

					@Override
					public void run() {
						Map<String, ClientContext> brokerList = loadAndAddBrokerListWatcher(BrokerChangedListener.this);
						eventBus.pubEvent(m_context, new Event(EventType.BROKER_LIST_CHANGED, brokerList));
					}
				});
			} else {
				if (m_brokerListenerAdded.compareAndSet(true, false)) {
					m_serviceCache.removeListener(this);
				}
			}
		}
	}

}
