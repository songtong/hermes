package com.ctrip.hermes.monitor.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.hermes.core.utils.StringUtils;

@Component
public class PartitionCheckerConfig {

	private static final Logger log = LoggerFactory.getLogger(PartitionCheckerConfig.class);

	private static final String PARTITION_ENABLE_PARTITION_SERVICE = "partition.enable.partition.service";

	private static final String PARTITION_ENABLE_DROP_UNUSED_TABLE = "partition.enable.drop.unused.table";

	private static final String PARTITION_CHECKER_EXCLUDE_TOPIC = "partition.checker.exclude.topic";

	private static final String PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY = "partition.pre.allocate.limitline.in.day";

	private static final String PARTITION_PRE_ALLOCATE_SIZE_IN_DAY = "partition.pre.allocate.size.in.day";

	private static final String PARTITION_PRE_ALLOCATE_MAX_COUNT = "partition.pre.allocate.max.count";

	private static final String PARTITION_RETENTION_HOUR = "partition.retention.hour";

	private static final String PARTITION_MAX_SIZE = "partition.max.size";

	private static final String PARTITION_SIZE_INCREASE_STEP = "partition.size.increase.step";

	private static final String DEFAULT_KEY = "__default__";

	private static final boolean DEFAULT_ENABLE_PARTITION_SERVICE = true;

	private static final boolean DEFAULT_ENABLE_DROP_UNUSED_TABLE = false;

	private static final int DEFAULT_PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY = 5;

	private static final int DEFAULT_PARTITION_PRE_ALLOCATE_SIZE_IN_DAY = 10;

	private static final int DEFAULT_PARTITION_PRE_ALLOCATE_MAX_COUNT = 100;

	private static final int DEFAULT_PARTITION_RETENTION_HOUR = 72;

	private static final int DEFAULT_PARTITION_MAX_SIZE = 10000000;

	private static final int DEFAULT_PARTITION_SIZE_INCREASE_STEP = 500000;

	private boolean m_enableDropUnusedTable;

	private boolean m_enablePartitionService;

	private AtomicReference<Set<String>> m_excludeTopic = new AtomicReference<Set<String>>(new HashSet<String>());

	private volatile int m_preAllocateLimitlineInDay;

	private volatile int m_preAllocateSizeInDay;

	private volatile int m_preAllocateMaxCount;

	private AtomicReference<Map<String, Integer>> m_partitionRetentionHour = new AtomicReference<Map<String, Integer>>(
	      new HashMap<String, Integer>());

	private AtomicReference<Map<String, Integer>> m_partitionMaxSize = new AtomicReference<Map<String, Integer>>(
	      new HashMap<String, Integer>());

	private AtomicReference<Map<String, Integer>> m_partitionSizeIncreaseStep = new AtomicReference<Map<String, Integer>>(
	      new HashMap<String, Integer>());

	@PostConstruct
	public void initialize() {
		Config config = ConfigService.getAppConfig();

		initPartitionCheckerConfigs(config);

		config.addChangeListener(new ConfigChangeListener() {
			@Override
			public void onChange(ConfigChangeEvent changeEvent) {
				checkConfigChangeEvent(changeEvent, PARTITION_ENABLE_PARTITION_SERVICE,
				      new ConfigChangeCallback<Boolean>() {
					      @Override
					      public void postConfigChanged(Boolean newValue) {
						      m_enablePartitionService = newValue;
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_ENABLE_DROP_UNUSED_TABLE,
				      new ConfigChangeCallback<Boolean>() {
					      @Override
					      public void postConfigChanged(Boolean newValue) {
						      m_enableDropUnusedTable = newValue;
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY,
				      new ConfigChangeCallback<Integer>() {
					      @Override
					      public void postConfigChanged(Integer newValue) {
						      m_preAllocateLimitlineInDay = newValue > 0 ? newValue : m_preAllocateLimitlineInDay;
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_PRE_ALLOCATE_SIZE_IN_DAY,
				      new ConfigChangeCallback<Integer>() {
					      @Override
					      public void postConfigChanged(Integer newValue) {
						      m_preAllocateSizeInDay = newValue > 0 ? newValue : m_preAllocateSizeInDay;
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_PRE_ALLOCATE_MAX_COUNT, new ConfigChangeCallback<Integer>() {
					@Override
					public void postConfigChanged(Integer newValue) {
						m_preAllocateMaxCount = newValue > 0 ? newValue : m_preAllocateMaxCount;
					}
				});
				checkConfigChangeEvent(changeEvent, PARTITION_RETENTION_HOUR,
				      new ConfigChangeCallback<Map<String, Integer>>() {
					      @Override
					      public void postConfigChanged(Map<String, Integer> newValue) {
						      m_partitionRetentionHour.set(newValue);
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_MAX_SIZE, new ConfigChangeCallback<Map<String, Integer>>() {
					@Override
					public void postConfigChanged(Map<String, Integer> newValue) {
						m_partitionMaxSize.set(newValue);
					}
				});
				checkConfigChangeEvent(changeEvent, PARTITION_SIZE_INCREASE_STEP,
				      new ConfigChangeCallback<Map<String, Integer>>() {
					      @Override
					      public void postConfigChanged(Map<String, Integer> newValue) {
						      m_partitionSizeIncreaseStep.set(newValue);
					      }
				      });
				checkConfigChangeEvent(changeEvent, PARTITION_CHECKER_EXCLUDE_TOPIC,
				      new ConfigChangeCallback<Set<String>>() {
					      @Override
					      public void postConfigChanged(Set<String> newValue) {
						      m_excludeTopic.set(newValue);
					      }
				      });
			}
		});
	}

	private void initPartitionCheckerConfigs(Config config) {
		m_enablePartitionService = config.getBooleanProperty(PARTITION_ENABLE_PARTITION_SERVICE,
		      DEFAULT_ENABLE_PARTITION_SERVICE);
		m_enableDropUnusedTable = config.getBooleanProperty(PARTITION_ENABLE_DROP_UNUSED_TABLE,
		      DEFAULT_ENABLE_DROP_UNUSED_TABLE);
		m_excludeTopic.set(JSON.parseObject(config.getProperty(PARTITION_CHECKER_EXCLUDE_TOPIC, "[]"),
		      new TypeReference<Set<String>>() {
		      }));

		m_preAllocateLimitlineInDay = config.getIntProperty(PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY,
		      DEFAULT_PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY);
		m_preAllocateSizeInDay = config.getIntProperty(PARTITION_PRE_ALLOCATE_SIZE_IN_DAY,
		      DEFAULT_PARTITION_PRE_ALLOCATE_SIZE_IN_DAY);
		m_preAllocateMaxCount = config.getIntProperty(PARTITION_PRE_ALLOCATE_MAX_COUNT,
		      DEFAULT_PARTITION_PRE_ALLOCATE_MAX_COUNT);

		m_partitionRetentionHour.set(JSON.parseObject(config.getProperty(PARTITION_RETENTION_HOUR, "{}"),
		      new TypeReference<Map<String, Integer>>() {
		      }));
		m_partitionMaxSize.set(JSON.parseObject(config.getProperty(PARTITION_MAX_SIZE, "{}"),
		      new TypeReference<Map<String, Integer>>() {
		      }));
		m_partitionSizeIncreaseStep.set(JSON.parseObject(config.getProperty(PARTITION_SIZE_INCREASE_STEP, "{}"),
		      new TypeReference<Map<String, Integer>>() {
		      }));
	}

	private interface ConfigChangeCallback<T> {
		public void postConfigChanged(T newValue);
	}

	private <T> void checkConfigChangeEvent(ConfigChangeEvent changeEvent, String key, ConfigChangeCallback<T> callback) {
		if (changeEvent.changedKeys().contains(key)) {
			T newValue = parseNewValue(changeEvent.getChange(key).getNewValue());
			try {
				if (newValue != null) {
					callback.postConfigChanged(newValue);
					log.info("Config({}) changed, new value: ({})", key, newValue);
				}
			} catch (Exception e) {
				log.error("Set new config({}) value failed. ", key, e);
			}
		}
	}

	private <T> T parseNewValue(String newValue) {
		try {
			if (!StringUtils.isBlank(newValue)) {
				return JSON.parseObject(newValue, new TypeReference<T>() {
				});
			}
		} catch (Exception e) {
			log.error("Parse new value failed: {}", newValue, e);
		}
		return null;
	}

	public void printPartitionConfigs() {
		log.info("============================================================================");
		log.info("=== {}: {}", PARTITION_ENABLE_PARTITION_SERVICE, m_enablePartitionService);
		log.info("=== {}: {}", PARTITION_ENABLE_DROP_UNUSED_TABLE, m_enableDropUnusedTable);
		log.info("=== {}: {}", PARTITION_CHECKER_EXCLUDE_TOPIC, m_excludeTopic);
		log.info("=== {}: {}", PARTITION_PRE_ALLOCATE_LIMITLINE_IN_DAY, m_preAllocateLimitlineInDay);
		log.info("=== {}: {}", PARTITION_PRE_ALLOCATE_SIZE_IN_DAY, m_preAllocateSizeInDay);
		log.info("=== {}: {}", PARTITION_PRE_ALLOCATE_MAX_COUNT, m_preAllocateMaxCount);
		log.info("=== {}: {}", PARTITION_RETENTION_HOUR, m_partitionRetentionHour);
		log.info("=== {}: {}", PARTITION_MAX_SIZE, m_partitionMaxSize);
		log.info("=== {}: {}", PARTITION_SIZE_INCREASE_STEP, m_partitionSizeIncreaseStep);
		log.info("============================================================================");
	}

	public boolean isPartitionServiceEnabled() {
		return m_enablePartitionService;
	}

	public boolean isDropUnusedTableEnabled() {
		return m_enableDropUnusedTable;
	}

	public boolean isTopicExcluded(String topic) {
		return m_excludeTopic.get().contains(topic);
	}

	public int getPreAllocateLimitlineInDay() {
		return m_preAllocateLimitlineInDay;
	}

	public int getPreAllocateSizeInDay() {
		return m_preAllocateSizeInDay;
	}

	public int getPreAllocateMaxCount() {
		return m_preAllocateMaxCount;
	}

	public int getRetentionHour(String topic) {
		return getConfigValue(topic, m_partitionRetentionHour.get(), DEFAULT_PARTITION_RETENTION_HOUR);
	}

	public int getPartitionMaxSize(String topic) {
		return getConfigValue(topic, m_partitionMaxSize.get(), DEFAULT_PARTITION_MAX_SIZE);
	}

	public int getPartitionSizeIncreaseStep(String topic) {
		return getConfigValue(topic, m_partitionSizeIncreaseStep.get(), DEFAULT_PARTITION_SIZE_INCREASE_STEP);
	}

	private int getConfigValue(String key, Map<String, Integer> configs, int defaultValue) {
		Integer value = configs.get(key);
		if (value == null) {
			value = configs.get(DEFAULT_KEY);
		}
		return value == null ? defaultValue : value;
	}
}
