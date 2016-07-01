package com.ctrip.hermes.metaservice.monitor.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfig;
import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfigDao;
import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfigEntity;
import com.ctrip.hermes.metaservice.model.ProducerMonitorConfig;
import com.ctrip.hermes.metaservice.model.ProducerMonitorConfigDao;
import com.ctrip.hermes.metaservice.model.ProducerMonitorConfigEntity;

@Named(type = MonitorConfigService.class)
public class DefaultMonitorConfigService implements MonitorConfigService, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultMonitorConfigService.class);

	private static final int CONFIG_CACHE_REFRESH_INTERVAL_MIN = 10;

	@Inject
	private ProducerMonitorConfigDao m_producerMonitorConfigDao;

	@Inject
	private ConsumerMonitorConfigDao m_consumerMonitorConfigDao;

	private AtomicReference<Map<String, ProducerMonitorConfig>> m_producerMonitorConfigCache = new AtomicReference<>();

	private AtomicReference<Map<Pair<String, String>, ConsumerMonitorConfig>> m_consumerMonitorConfigCache = new AtomicReference<>();

	private AtomicLong m_lastProducerConfigRefreshed = new AtomicLong(0);

	private AtomicLong m_lastConsumerConfigRefreshed = new AtomicLong(0);

	private String m_updateConsumerMonitorConfigLock = "Non-sense-clock";

	private String m_updateProducerMonitorConfigLock = "Non-sense-plock";

	private synchronized boolean isExpired(AtomicLong timestamp, int validTime) {
		long now = System.currentTimeMillis();
		if (now - timestamp.get() > TimeUnit.MINUTES.toMillis(validTime)) {
			timestamp.set(now);
			return true;
		}
		return false;
	}

	private void doRefreshProducerMonitorConfigCache() {
		try {
			Map<String, ProducerMonitorConfig> newCache = new HashMap<>();
			for (ProducerMonitorConfig cfg : m_producerMonitorConfigDao.list(ProducerMonitorConfigEntity.READSET_FULL)) {
				newCache.put(cfg.getTopic(), cfg);
			}
			m_producerMonitorConfigCache.set(newCache);
		} catch (Exception e) {
			log.error("Refresh producer monitor config failed.", e);
		}
	}

	private void doRefreshConsumerMonitorConfigCache() {
		try {
			Map<Pair<String, String>, ConsumerMonitorConfig> newCache = new HashMap<>();
			for (ConsumerMonitorConfig cfg : m_consumerMonitorConfigDao.list(ConsumerMonitorConfigEntity.READSET_FULL)) {
				newCache.put(new Pair<String, String>(cfg.getTopic(), cfg.getConsumer()), cfg);
			}
			m_consumerMonitorConfigCache.set(newCache);
		} catch (Exception e) {
			log.error("Refresh consumer monitor config failed.", e);
		}
	}

	private void refreshProducerMonitorConfigCacheIfNeeded() {
		if (isExpired(m_lastProducerConfigRefreshed, CONFIG_CACHE_REFRESH_INTERVAL_MIN)) {
			doRefreshProducerMonitorConfigCache();
		}
	}

	private void refreshConsumerMonitorConfigCacheIfNeeded() {
		if (isExpired(m_lastConsumerConfigRefreshed, CONFIG_CACHE_REFRESH_INTERVAL_MIN)) {
			doRefreshConsumerMonitorConfigCache();
		}
	}

	@Override
	public ProducerMonitorConfig getProducerMonitorConfig(String topic) {
		refreshProducerMonitorConfigCacheIfNeeded();
		return m_producerMonitorConfigCache.get().get(topic);
	}

	@Override
	public List<ProducerMonitorConfig> listProducerMonitorConfig() {
		refreshProducerMonitorConfigCacheIfNeeded();
		return new ArrayList<ProducerMonitorConfig>(m_producerMonitorConfigCache.get().values());
	}

	@Override
	public ConsumerMonitorConfig getConsumerMonitorConfig(String topic, String consumer) {
		refreshConsumerMonitorConfigCacheIfNeeded();
		return m_consumerMonitorConfigCache.get().get(new Pair<String, String>(topic, consumer));
	}

	@Override
	public List<ConsumerMonitorConfig> getConsumerMonitorConfig(String topic) {
		refreshConsumerMonitorConfigCacheIfNeeded();
		List<ConsumerMonitorConfig> list = new ArrayList<>();
		for (Entry<Pair<String, String>, ConsumerMonitorConfig> entry : m_consumerMonitorConfigCache.get().entrySet()) {
			if (entry.getKey().getKey().equals(topic)) {
				list.add(entry.getValue());
			}
		}
		return list;
	}

	@Override
	public List<ConsumerMonitorConfig> listConsumerMonitorConfig() {
		refreshConsumerMonitorConfigCacheIfNeeded();
		return new ArrayList<ConsumerMonitorConfig>(m_consumerMonitorConfigCache.get().values());
	}

	@Override
	public void setProducerMonitorConfig(ProducerMonitorConfig config) {
		if (StringUtils.isBlank(config.getTopic())) {
			throw new IllegalArgumentException("Producer monitor config's topic name can not be empty.");
		}

		synchronized (m_updateProducerMonitorConfigLock) {
			ProducerMonitorConfig old = m_producerMonitorConfigCache.get().get(config.getTopic());
			if (old != null) {
				config.setId(old.getId());
				try {
					m_producerMonitorConfigDao.updateByPK(config, ProducerMonitorConfigEntity.UPDATESET_FULL);
				} catch (Exception e) {
					log.error("Update producer monitor config failed: {}", config, e);
				}
			} else {
				try {
					m_producerMonitorConfigDao.insert(config);
				} catch (Exception e) {
					log.error("Add producer monitor config failed: {}", config, e);
				}
			}

			m_producerMonitorConfigCache.get().put(config.getTopic(), config);
		}
	}

	@Override
	public void setConsumerMonitorConfig(ConsumerMonitorConfig config) {
		if (StringUtils.isBlank(config.getTopic()) || StringUtils.isBlank(config.getConsumer())) {
			throw new IllegalArgumentException("Consumer monitor config's topic or consumer name can not be empty.");
		}

		synchronized (m_updateConsumerMonitorConfigLock) {
			Pair<String, String> key = new Pair<String, String>(config.getTopic(), config.getConsumer());
			ConsumerMonitorConfig old = m_consumerMonitorConfigCache.get().get(key);

			if (old != null) {
				config.setId(old.getId());
				try {
					m_consumerMonitorConfigDao.updateByPK(config, ConsumerMonitorConfigEntity.UPDATESET_FULL);
				} catch (Exception e) {
					log.error("Update consumer monitor config failed: {}", config, e);
				}
			} else {
				try {
					m_consumerMonitorConfigDao.insert(config);
				} catch (Exception e) {
					log.error("Add consumer monitor config failed: {}", config, e);
				}
			}

			m_consumerMonitorConfigCache.get().put(key, config);
		}
	}

	@Override
	public void deleteProducerMonitorConfig(String topic) {
		synchronized (m_updateProducerMonitorConfigLock) {
			ProducerMonitorConfig config = m_producerMonitorConfigCache.get().get(topic);
			if (config != null) {
				try {
					m_producerMonitorConfigDao.deleteByPK(config);
				} catch (Exception e) {
					log.error("Delete producer monitor config failed: {}", config, e);
				}
			}

			m_producerMonitorConfigCache.get().remove(topic);
		}
	}

	@Override
	public void deleteConsumerMonitorConfig(String topic, String consumer) {
		synchronized (m_updateConsumerMonitorConfigLock) {
			Pair<String, String> key = new Pair<String, String>(topic, consumer);
			ConsumerMonitorConfig config = m_consumerMonitorConfigCache.get().get(key);
			if (config != null) {
				try {
					m_consumerMonitorConfigDao.deleteByPK(config);
				} catch (Exception e) {
					log.error("Delete consumer monitor config failed: {}", config, e);
				}
			}

			m_consumerMonitorConfigCache.get().remove(key);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		doRefreshProducerMonitorConfigCache();
		doRefreshConsumerMonitorConfigCache();
	}
}
