package com.ctrip.hermes.metaservice.service;

import java.util.HashMap;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.dal.CachedAppDao;
import com.ctrip.hermes.metaservice.dal.CachedCodecDao;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedProducerDao;
import com.ctrip.hermes.metaservice.dal.CachedSchemaDao;
import com.ctrip.hermes.metaservice.dal.CachedServerDao;
import com.ctrip.hermes.metaservice.dal.CachedStorageDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.google.common.cache.CacheStats;

@Named
public class CacheDalService {

	@Inject
	private CachedAppDao appDao;

	@Inject
	private CachedCodecDao codecDao;

	@Inject
	private CachedConsumerGroupDao consumerDao;

	@Inject
	private CachedDatasourceDao dsDao;

	@Inject
	private CachedEndpointDao endpointDao;

	@Inject
	private CachedPartitionDao partitionDao;

	@Inject
	private CachedProducerDao producerDao;

	@Inject
	private CachedSchemaDao schemaDao;

	@Inject
	private CachedServerDao serverDao;

	@Inject
	private CachedStorageDao storageDao;

	@Inject
	private CachedTopicDao topicDao;

	public Map<String, CacheStats> getCacheStats() {
		Map<String, CacheStats> result = new HashMap<String, CacheStats>();
		result.putAll(appDao.getStats());
		result.putAll(codecDao.getStats());
		result.putAll(consumerDao.getStats());
		result.putAll(dsDao.getStats());
		result.putAll(endpointDao.getStats());
		result.putAll(producerDao.getStats());
		result.putAll(schemaDao.getStats());
		result.putAll(serverDao.getStats());
		result.putAll(storageDao.getStats());
		result.putAll(topicDao.getStats());
		result.putAll(partitionDao.getStats());
		return result;
	}

	public void invalidateAll() {
		appDao.invalidateAll();
		codecDao.invalidateAll();
		consumerDao.invalidateAll();
		dsDao.invalidateAll();
		endpointDao.invalidateAll();
		producerDao.invalidateAll();
		schemaDao.invalidateAll();
		serverDao.invalidateAll();
		storageDao.invalidateAll();
		topicDao.invalidateAll();
		partitionDao.invalidateAll();
	}
}
