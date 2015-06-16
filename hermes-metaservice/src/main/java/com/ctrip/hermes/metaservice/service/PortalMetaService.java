package com.ctrip.hermes.metaservice.service;

import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

public interface PortalMetaService extends MetaService {
	public Storage findStorageByTopic(String topicName);

	public Topic findTopicById(long topicId);

	public Topic findTopicByName(String topicName);

	public Codec findCodecByTopic(String topicName);

	public Codec findCodecByType(String codecType);

	public Map<String, Codec> getCodecs();

	public Datasource findDatasource(String storageType, String datasourceId);

	public Meta getMeta();

	public List<Partition> findPartitionsByTopic(String topicName);

	public Map<String, Endpoint> getEndpoints();

	public Map<String, Storage> getStorages();

	public void addEndpoint(Endpoint endpoint) throws Exception;

	public void deleteEndpoint(String endpointId) throws Exception;

	public void addDatasource(Datasource datasource) throws Exception;

	public void deleteDatasource(String datasourceId) throws Exception;

	public Map<String, Topic> getTopics();
}
