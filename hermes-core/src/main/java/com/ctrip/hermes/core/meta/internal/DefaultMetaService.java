package com.ctrip.hermes.core.meta.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

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
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor2;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	private MetaManager m_manager;

	@Inject
	private CoreConfig m_config;

	private AtomicReference<Meta> m_metaCache = new AtomicReference<Meta>();

	protected Meta getMeta() {
		return m_metaCache.get();
	}

	protected MetaProxy getMetaProxy() {
		return m_manager.getMetaProxy();
	}

	protected Topic findTopic(String topicName, Meta meta) {
		Topic topic = meta.findTopic(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s not found", topicName));
		}
		return topic;
	}

	@Override
	public String findEndpointTypeByTopic(String topicName) {
		return findTopic(topicName, getMeta()).getEndpointType();
	}

	@Override
	public List<Partition> listPartitionsByTopic(String topicName) {
		return findTopic(topicName, getMeta()).getPartitions();
	}

	@Override
	public Storage findStorageByTopic(String topicName) {
		Meta meta = getMeta();
		Topic topic = findTopic(topicName, meta);
		String storageType = topic.getStorageType();
		return meta.findStorage(storageType);
	}

	@Override
	public Codec findCodecByTopic(String topicName) {
		Meta meta = getMeta();
		Topic topic = findTopic(topicName, meta);
		String codeType = topic.getCodecType();
		return meta.getCodecs().get(codeType);
	}

	@Override
	public Partition findPartitionByTopicAndPartition(String topicName, int partitionId) {
		return findTopic(topicName, getMeta()).findPartition(partitionId);
	}

	@Override
	public List<Topic> listTopicsByPattern(String topicPattern) {
		if (StringUtils.isBlank(topicPattern)) {
			throw new RuntimeException("Topic pattern can not be null or blank");
		}

		topicPattern = StringUtils.trim(topicPattern);

		Meta meta = getMeta();
		List<Topic> matchedTopics = new ArrayList<Topic>();

		Collection<Topic> topics = meta.getTopics().values();

		for (Topic topic : topics) {
			if (isTopicMatch(topicPattern, topic.getName())) {
				matchedTopics.add(topic);
			}
		}

		return matchedTopics;
	}

	boolean isTopicMatch(String topicPattern, String topic) {
		boolean isMatch = false;
		if (topic.equalsIgnoreCase(topicPattern)) {
			isMatch = true;
		} else {
			if (topicPattern.indexOf("*") >= 0 || topicPattern.indexOf("#") >= 0) {
				String pattern = buildMatchPattern(topicPattern);
				isMatch = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(topic).matches();
			}
		}
		return isMatch;
	}

	private String buildMatchPattern(String topicPattern) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < topicPattern.length(); i++) {
			if (i == 0) {
				sb.append("^");
			}

			char curChar = topicPattern.charAt(i);
			if (curChar == '*') {
				sb.append("\\w+");
			} else if (curChar == '#') {
				sb.append("(\\w\\.?)+");
			} else {
				sb.append(curChar);
			}

			if (i == topicPattern.length() - 1) {
				sb.append("$");
			}
		}

		return sb.toString();
	}

	@Override
	public Topic findTopicByName(String topicName) {
		try {
			return findTopic(topicName, getMeta());
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public int translateToIntGroupId(String topicName, String groupName) {
		Topic topic = findTopic(topicName, getMeta());

		if (containsConsumerGroup(topicName, groupName)) {
			ConsumerGroup consumerGroup = topic.findConsumerGroup(groupName);
			return consumerGroup.getId();
		} else {
			throw new RuntimeException(String.format("Consumer group not found for topic %s and group %s", topicName,
			      groupName));
		}
	}

	@Override
	public List<Datasource> listAllMysqlDataSources() {
		Meta meta = getMeta();
		final List<Datasource> dataSources = new ArrayList<Datasource>();

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

	protected void refreshMeta() {
		int maxTries = 10;
		RuntimeException exception = null;

		for (int i = 0; i < maxTries; i++) {
			try {
				Meta meta = m_manager.loadMeta();
				if (meta != null) {
					m_metaCache.set(meta);
					return;
				}
			} catch (RuntimeException e) {
				exception = e;
			}

			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		if (exception != null) {
			log.warn("Failed to refresh meta from meta-server for {} times", maxTries);
			throw exception;
		}
	}

	@Override
	public int getAckTimeoutSecondsByTopicAndConsumerGroup(String topicName, String groupId) {
		Topic topic = findTopic(topicName, getMeta());

		if (containsConsumerGroup(topicName, groupId)) {
			ConsumerGroup consumerGroup = topic.findConsumerGroup(groupId);
			if (consumerGroup.getAckTimeoutSeconds() == null) {
				return topic.getAckTimeoutSeconds();
			} else {
				return consumerGroup.getAckTimeoutSeconds();
			}
		} else {
			throw new RuntimeException(String.format("Consumer group %s for topic %s not found", groupId, topicName));
		}

	}

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		return getMetaProxy().tryAcquireConsumerLease(tpg, sessionId);
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		return getMetaProxy().tryRenewConsumerLease(tpg, lease, sessionId);
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
	      int brokerPort) {
		return getMetaProxy().tryRenewBrokerLease(topic, partition, lease, sessionId, brokerPort);
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
		return getMetaProxy().tryAcquireBrokerLease(topic, partition, sessionId, brokerPort);
	}

	@Override
	public void initialize() throws InitializationException {
		refreshMeta();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("RefreshMeta", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      refreshMeta();
				      } catch (Exception e) {
					      log.warn("Failed to refresh meta", e);
				      }
			      }

		      }, m_config.getMetaCacheRefreshIntervalSeconds(), m_config.getMetaCacheRefreshIntervalSeconds(),
		            TimeUnit.SECONDS);
	}

	@Override
	public String getAvroSchemaRegistryUrl() {
		Codec avroCodec = getMeta().findCodec(Codec.AVRO);
		return avroCodec.getProperties().get(m_config.getAvroSchemaRetryUrlKey()).getValue();
	}

	@Override
	public Endpoint findEndpointByTopicAndPartition(String topic, int partition) {
		return getMeta().findEndpoint(findTopic(topic, getMeta()).findPartition(partition).getEndpoint());
	}

	@Override
	public RetryPolicy findRetryPolicyByTopicAndGroup(String topicName, String groupId) {
		Topic topic = findTopic(topicName, getMeta());

		if (containsConsumerGroup(topicName, groupId)) {
			ConsumerGroup consumerGroup = topic.findConsumerGroup(groupId);

			String retryPolicyValue = consumerGroup.getRetryPolicy();
			if (StringUtils.isBlank(retryPolicyValue)) {
				retryPolicyValue = topic.getConsumerRetryPolicy();
			}

			return RetryPolicyFactory.create(retryPolicyValue);
		} else {

			throw new RuntimeException(String.format("Consumer group %s for topic %s not found", groupId, topicName));
		}
	}

	@Override
	public List<SubscriptionView> listSubscriptions(String status) {
		return getMetaProxy().listSubscriptions(status);
	}

	@Override
	public boolean containsEndpoint(Endpoint endpoint) {
		return getMeta().getEndpoints().containsKey(endpoint.getId());
	}

	@Override
	public boolean containsConsumerGroup(String topicName, String groupId) {
		Topic topic = findTopic(topicName, getMeta());

		ConsumerGroup consumerGroup = topic.findConsumerGroup(groupId);

		if (consumerGroup == null) {
			return false;
		}

		return true;
	}

	@Override
	public String getZookeeperList() {
		Map<String, Storage> storages = getMeta().getStorages();
		for (Storage storage : storages.values()) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("zookeeper.connect".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

	@Override
	public String getKafkaBrokerList() {
		Map<String, Storage> storages = getMeta().getStorages();
		for (Storage storage : storages.values()) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("bootstrap.servers".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

}
