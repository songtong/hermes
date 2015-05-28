package com.ctrip.hermes.core.meta;

import java.util.List;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MetaService {

	Storage findStorageByTopic(String topic);

	List<Datasource> listAllMysqlDataSources();

	Endpoint findEndpointByTopicAndPartition(String topic, int partition);

	String findEndpointTypeByTopic(String topic);

	List<Partition> listPartitionsByTopic(String topic);

	Partition findPartitionByTopicAndPartition(String topic, int partition);

	Codec findCodecByTopic(String topic);

	List<Topic> listTopicsByPattern(String topicPattern);

	Topic findTopicByName(String topic);

	int translateToIntGroupId(String topic, String groupId);

	int getAckTimeoutSecondsTopicAndConsumerGroup(String topic, String groupId);

	void refreshMeta(Meta meta);

	RetryPolicy findRetryPolicyByTopicAndGroup(String topic, String groupId);

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId);

	String findAvroSchemaRegistryUrl();

}
