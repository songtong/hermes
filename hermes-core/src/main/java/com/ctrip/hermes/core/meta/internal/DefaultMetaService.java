package com.ctrip.hermes.core.meta.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.message.retry.RetryPolicy;
import com.ctrip.hermes.core.message.retry.RetryPolicyFactory;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor2;

@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	private MetaManager m_manager;

	@Inject
	private CoreConfig m_config;

	private ScheduledExecutorService executor;

	private AtomicReference<Meta> m_metaCache = new AtomicReference<>();

	@Override
	public String findEndpointTypeByTopic(String topicName) {
		Meta meta = m_metaCache.get();
		Topic topic = meta.getTopics().get(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}

		return topic.getEndpointType();
	}

	@Override
	public List<Partition> listPartitionsByTopic(String topicName) {
		Meta meta = m_metaCache.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null) {
			return topic.getPartitions();
		} else {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}
	}

	@Override
	public Storage findStorageByTopic(String topicName) {
		Meta meta = m_metaCache.get();
		Topic topic = meta.findTopic(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}
		String storageType = topic.getStorageType();
		return meta.findStorage(storageType);
	}

	@Override
	public Codec findCodecByTopic(String topicName) {
		Meta meta = m_metaCache.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null) {
			String codeType = topic.getCodecType();
			return meta.getCodecs().get(codeType);
		} else {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}
	}

	@Override
	public Partition findPartitionByTopicAndPartition(String topicName, int partitionId) {
		Meta meta = m_metaCache.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null) {
			return topic.findPartition(partitionId);
		} else {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}
	}

	public List<Topic> listTopicsByPattern(String topicPattern) {
		if (StringUtils.isBlank(topicPattern)) {
			throw new RuntimeException("Topic pattern can not be null or blank");
		}

		topicPattern = StringUtils.trim(topicPattern);

		boolean hasWildcard = topicPattern.endsWith("*");

		if (hasWildcard) {
			topicPattern = topicPattern.substring(0, topicPattern.length() - 1);
		}

		Meta meta = m_metaCache.get();
		List<Topic> matchedTopics = new ArrayList<>();

		Collection<Topic> topics = meta.getTopics().values();

		for (Topic topic : topics) {
			if (hasWildcard) {
				if (StringUtils.startsWithIgnoreCase(topic.getName(), topicPattern)) {
					matchedTopics.add(topic);
				}
			} else {
				if (StringUtils.equalsIgnoreCase(topic.getName(), topicPattern)) {
					matchedTopics.add(topic);
				}
			}
		}

		return matchedTopics;
	}

	@Override
	public Topic findTopicByName(String topic) {
		Meta meta = m_metaCache.get();
		return meta.findTopic(topic);
	}

	@Override
	public int translateToIntGroupId(String topicName, String groupName) {
		Topic topic = findTopicByName(topicName);

		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}

		ConsumerGroup consumerGroup = topic.findConsumerGroup(groupName);

		if (consumerGroup != null) {
			return consumerGroup.getId();
		} else {
			throw new RuntimeException(String.format("Consumer group not found for topic %s and group %s", topicName,
			      groupName));
		}
	}

	@Override
	public List<Datasource> listAllMysqlDataSources() {
		Meta meta = m_metaCache.get();
		final List<Datasource> dataSources = new ArrayList<>();

		meta.accept(new BaseVisitor2() {

			@Override
			protected void visitDatasourceChildren(Datasource ds) {
				Storage storage = getAncestor(2);

				if (StringUtils.equalsIgnoreCase(Storage.MYSQL, storage.getType())) {
					dataSources.add(ds);
				}

				super.visitDatasourceChildren(ds);
			}

		});

		return dataSources;
	}

	public void refresh(){
		refreshMeta(m_manager.loadMeta());
	}
	
	private void refreshMeta(Meta meta) {
		m_metaCache.set(meta);
	}

	@Override
	public int getAckTimeoutSecondsTopicAndConsumerGroup(String topicName, String groupId) {
		Topic topic = findTopicByName(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}

		ConsumerGroup consumerGroup = topic.findConsumerGroup(groupId);

		if (consumerGroup == null) {
			throw new RuntimeException(String.format("Consumer group %s for topic %s not found", groupId, topicName));
		}

		if (consumerGroup.getAckTimeoutSeconds() == null) {
			return topic.getAckTimeoutSeconds();
		} else {
			return consumerGroup.getAckTimeoutSeconds();
		}

	}

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		return m_manager.getMetaProxy().tryAcquireConsumerLease(tpg, sessionId);
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		return m_manager.getMetaProxy().tryRenewConsumerLease(tpg, lease, sessionId);
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
	      int brokerPort) {
		return m_manager.getMetaProxy().tryRenewBrokerLease(topic, partition, lease, sessionId, brokerPort);
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
		return m_manager.getMetaProxy().tryAcquireBrokerLease(topic, partition, sessionId, brokerPort);
	}

	@Override
	public void initialize() throws InitializationException {
		refreshMeta(m_manager.loadMeta());
		executor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("RefreshMeta", true));
		executor
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      refresh();
				      } catch (Exception e) {
					      log.warn("Failed to refresh meta", e);
				      }
			      }

		      }, m_config.getMetaCacheRefreshIntervalMinutes(), m_config.getMetaCacheRefreshIntervalMinutes(),
		            TimeUnit.MINUTES);
	}

	@Override
	public String findAvroSchemaRegistryUrl() {
		Codec avroCodec = m_metaCache.get().findCodec(Codec.AVRO);
		return avroCodec.getProperties().get(m_config.getAvroSchemaRetryUrlKey()).getValue();
	}

	@Override
	public Endpoint findEndpointByTopicAndPartition(String topic, int partition) {
		return m_metaCache.get().findEndpoint(findTopicByName(topic).findPartition(partition).getEndpoint());
	}

	@Override
	public RetryPolicy findRetryPolicyByTopicAndGroup(String topicName, String groupId) {
		Topic topic = findTopicByName(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}

		ConsumerGroup consumerGroup = topic.findConsumerGroup(groupId);

		if (consumerGroup == null) {
			throw new RuntimeException(String.format("Consumer group %s for topic %s not found", groupId, topicName));
		}

		String retryPolicyValue = consumerGroup.getRetryPolicy();
		if (StringUtils.isBlank(retryPolicyValue)) {
			retryPolicyValue = topic.getConsumerRetryPolicy();
		}

		return RetryPolicyFactory.create(retryPolicyValue);
	}

	@Override
   public List<SubscriptionView> listSubscriptions() {
		return m_manager.getMetaProxy().listSubscriptions();
	}

	@Override
   public List<SchemaView> listSchemas() {
		return m_manager.getMetaProxy().listSchemas();
	}

}
