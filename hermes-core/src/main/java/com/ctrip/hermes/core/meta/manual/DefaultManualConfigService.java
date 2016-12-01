package com.ctrip.hermes.core.meta.manual;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.transport.command.v6.FetchManualConfigCommandV6;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.core.transport.monitor.FetchManualConfigResultMonitor;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ManualConfigProvider;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ManualConfigService.class)
public class DefaultManualConfigService implements ManualConfigService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultManualConfigService.class);

	@Inject
	private ManualConfigProvider m_configProvider;

	@Inject
	private FetchManualConfigResultMonitor m_configResultMonitor;

	private AtomicReference<ManualConfig> m_config = new AtomicReference<>();

	private ManualConfiguredMetaProxy m_metaProxy = new ManualConfiguredMetaProxy();

	private boolean m_inBroker = false;

	private String m_brokersStr;

	private List<Endpoint> m_brokerEndpoints;

	@Override
	public synchronized void configure(ManualConfig newConfig) {
		if (newConfig != null && newConfig.getMeta() != null) {
			ManualConfig oldConfig = m_config.get();
			if (oldConfig == null || !oldConfig.equals(newConfig)) {
				if (m_inBroker) {
					newConfig.toBytes();
				}
				m_metaProxy.setManualConfig(newConfig);
				m_config.set(newConfig);
				log.info("New manual config updated(version:{}).", newConfig.getVersion());
			}
		}
	}

	@Override
	public Meta getMeta() {
		ManualConfig config = m_config.get();
		return config == null ? null : config.getMeta();
	}

	@Override
	public MetaProxy getMetaProxy() {
		return getMeta() == null ? null : m_metaProxy;
	}

	@Override
	public synchronized void reset() {
		m_config.set(null);
		m_metaProxy.reset();
	}

	@Override
	public void initialize() throws InitializationException {
		m_inBroker = hasBrokerComponents();

		checkManualConfigureMode();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("ManualConfigFetcher", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      checkManualConfigureMode();
				      } catch (Exception e) {
					      log.warn("Check manual config failed.");
				      }
			      }

		      }, 5, 5, TimeUnit.SECONDS);
	}

	private void checkManualConfigureMode() {
		if (m_configProvider.isManualConfigureModeOn()) {
			if (getMeta() == null) {
				log.info("Entering manual config mode");
			}

			ManualConfig manualConfig = m_inBroker ? fetchManualConfigFromConfigProvider() : fetchManualConfigFromBroker();
			if (manualConfig != null) {
				configure(manualConfig);
			}
		} else {
			if (getMeta() != null) {
				log.info("Exiting from manual config mode");
				reset();
			}
		}
	}

	private ManualConfig fetchManualConfigFromBroker() {
		if (m_configProvider.getBrokers() != null && !StringUtils.equals(m_brokersStr, m_configProvider.getBrokers())) {
			m_brokerEndpoints = convertToEndpoints(m_configProvider.getBrokers());
			m_brokersStr = m_configProvider.getBrokers();
		}

		if (m_brokerEndpoints != null && !m_brokerEndpoints.isEmpty()) {

			Collections.shuffle(m_brokerEndpoints);
			EndpointClient endpointClient = PlexusComponentLocator.lookup(EndpointClient.class);

			for (Endpoint endpoint : m_brokerEndpoints) {
				try {
					FetchManualConfigCommandV6 cmd = new FetchManualConfigCommandV6();
					ManualConfig oldConfig = getConfig();
					cmd.setVersion(oldConfig == null ? FetchManualConfigCommandV6.UNSET_VERSION : oldConfig.getVersion());
					Future<ManualConfig> future = m_configResultMonitor.monitor(cmd);
					for (int i = 0; i < 3; i++) {
						if (endpointClient.writeCommand(endpoint, cmd)) {
							try {
								ManualConfig config = future.get(1, TimeUnit.SECONDS);
								if (config != null) {
									log.info("Fetched manual config from broker {}:{}", endpoint.getHost(), endpoint.getPort());
								}
								return config;
							} catch (TimeoutException e) {
								// ignore
							}
						} else {
							TimeUnit.MILLISECONDS.sleep(100);
						}
					}
				} catch (Exception e) {
					log.warn("Fetch manual config from broker {}:{} failed", endpoint.getHost(), endpoint.getPort());
				}
			}
		} else {
			log.info("Can't fetch manual config, since no brokers found.");
		}

		return null;
	}

	private List<Endpoint> convertToEndpoints(String brokers) {
		try {
			List<String> ipPortStrList = JSON.parseObject(brokers, TypeReference.LIST_STRING);
			if (ipPortStrList != null && !ipPortStrList.isEmpty()) {
				List<Endpoint> endpoints = new ArrayList<>();
				for (String ipPortStr : ipPortStrList) {
					String[] splits = ipPortStr.split(":");
					String ip = splits[0].trim();
					int port = Integer.parseInt(splits[1].trim());
					Endpoint endpoint = new Endpoint(UUID.randomUUID().toString());
					endpoint.setHost(ip);
					endpoint.setPort(port);
					endpoint.setType(Endpoint.BROKER);
					endpoints.add(endpoint);
				}
				return endpoints;
			}
		} catch (Exception e) {
			log.warn("Parse brokers string failed.", e);
		}

		return null;
	}

	private ManualConfig fetchManualConfigFromConfigProvider() {
		String manualConfigStr = m_configProvider.fetchManualConfig();
		if (manualConfigStr != null) {
			try {
				return JSON.parseObject(manualConfigStr, ManualConfig.class);
			} catch (RuntimeException e) {
				log.warn("Fetch manual config failed.");
			}
		}
		return null;
	}

	private boolean hasBrokerComponents() {
		boolean hasBrokerComponents = true;
		try {
			Class<?> brokerRegistry = Class.forName("com.ctrip.hermes.broker.registry.BrokerRegistry");
			if (brokerRegistry == null) {
				hasBrokerComponents = false;
			}
		} catch (ClassNotFoundException e) {
			hasBrokerComponents = false;
		}
		return hasBrokerComponents;
	}

	@Override
	public ManualConfig getConfig() {
		return m_config.get();
	}

	@Override
	public boolean isManualConfigModeOn() {
		return m_configProvider.isManualConfigureModeOn();
	}
}
