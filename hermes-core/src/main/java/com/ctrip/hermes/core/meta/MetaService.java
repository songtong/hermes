package com.ctrip.hermes.core.meta;

import java.util.List;

import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MetaService {

	Codec findCodecByTopic(String topic);

	Endpoint findEndpointByTopicAndPartition(String topic, int partition);

	String findEndpointTypeByTopic(String topic);

	Partition findPartitionByTopicAndPartition(String topic, int partition);

	RetryPolicy findRetryPolicyByTopicAndGroup(String topic, String groupId);

	Storage findStorageByTopic(String topic);

	Topic findTopicByName(String topic);

	int getAckTimeoutSecondsByTopicAndConsumerGroup(String topic, String groupId);

	List<Datasource> listAllMysqlDataSources();

	List<Partition> listPartitionsByTopic(String topic);

	List<Topic> listTopicsByPattern(String topicPattern);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId, int brokerPort);

	int translateToIntGroupId(String topic, String groupId);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort);

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	List<SubscriptionView> listSubscriptions(String status);

	boolean containsEndpoint(Endpoint endpoint);

	boolean containsConsumerGroup(String topicName, String groupId);

	String getAvroSchemaRegistryUrl();

	String getZookeeperList();

	String getKafkaBrokerList();
}
