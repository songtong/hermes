package com.ctrip.hermes.core.meta.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;
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

	private static final int REFRESH_PERIOD_MINUTES = 1;

	@Inject
	private MetaManager m_manager;

	private ScheduledExecutorService executor;

	private AtomicReference<Meta> m_meta = new AtomicReference<>();

	private AtomicReference<Map<Long, Topic>> m_topics = new AtomicReference<>();

	@Override
	public String getEndpointType(String topicName) {
		Meta meta = m_meta.get();
		Topic topic = meta.getTopics().get(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s is not found", topicName));
		}
		List<Partition> partitions = topic.getPartitions();
		if (partitions == null || partitions.size() == 0) {
			throw new RuntimeException(String.format("Partitions for topic %s is not found", topicName));
		}
		String endpointId = partitions.get(0).getEndpoint();
		Endpoint endpoint = meta.getEndpoints().get(endpointId);
		if (endpoint == null) {
			throw new RuntimeException(String.format("Endpoint for topic %s is not found", topicName));
		} else {
			return endpoint.getType();
		}
	}

	@Override
	public List<Partition> getPartitions(String topicName) {
		Meta meta = m_meta.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null) {
			return topic.getPartitions();
		}
		return null;
	}

	@Override
	public Endpoint findEndpoint(String endpointId) {
		Meta meta = m_meta.get();
		return meta.findEndpoint(endpointId);
	}

	@Override
	public Storage findStorage(String topic) {
		Meta meta = m_meta.get();
		String storageType = meta.findTopic(topic).getStorageType();
		return meta.findStorage(storageType);
	}

	@Override
	public Codec getCodecByTopic(String topicName) {
		Meta meta = m_meta.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null) {
			String codeType = topic.getCodecType();
			return meta.getCodecs().get(codeType);
		} else
			return null;
	}

	@Override
	public Partition findPartition(String topicName, int partitionId) {
		Meta meta = m_meta.get();
		Topic topic = meta.findTopic(topicName);
		if (topic != null)
			return topic.findPartition(partitionId);
		else
			return null;
	}

	public List<Topic> findTopicsByPattern(String topicPattern) {
		Meta meta = m_meta.get();
		List<Topic> matchedTopics = new ArrayList<>();

		Collection<Topic> topics = meta.getTopics().values();

		Pattern pattern = Pattern.compile(topicPattern);

		for (Topic topic : topics) {
			if (pattern.matcher(topic.getName()).matches()) {
				matchedTopics.add(topic);
			}
		}

		return matchedTopics;
	}

	@Override
	public Topic findTopic(String topic) {
		Meta meta = m_meta.get();
		return meta.findTopic(topic);
	}

	@Override
	public List<Partition> getPartitions(String topic, String groupId) {
		// TODO 对一个group的不同机器返回不一样的Partition
		return getPartitions(topic);
	}

	@Override
	public int getGroupIdInt(String groupName) {
		Meta meta = m_meta.get();
		// TODO groupIdStr唯一
		for (Topic topic : meta.getTopics().values()) {
			for (ConsumerGroup group : topic.getConsumerGroups()) {
				if (group.getName().equals(groupName)) {
					return group.getId();
				}
			}
		}

		// TODO
		return 100;
	}

	@Override
	public List<Datasource> listMysqlDataSources() {
		Meta meta = m_meta.get();
		// TODO
		final List<Datasource> dataSources = new ArrayList<>();

		meta.accept(new BaseVisitor2() {

			@Override
			protected void visitDatasourceChildren(Datasource ds) {
				Storage storage = getAncestor(2);

				if ("mysql".equalsIgnoreCase(storage.getType())) {
					dataSources.add(ds);
				}

				super.visitDatasourceChildren(ds);
			}

		});

		return dataSources;
	}

	public void refreshMeta(Meta meta) {
		final HashMap<Long, Topic> topics = new HashMap<>();

		meta.accept(new BaseVisitor2() {

			@Override
			protected void visitTopicChildren(Topic topic) {
				topics.put(topic.getId(), topic);

				super.visitTopicChildren(topic);
			}

		});

		m_meta.set(meta);
		m_topics.set(topics);
	}

	@Override
	public Topic findTopic(long topicId) {
		return m_topics.get().get(topicId);
	}

	@Override
	public int getAckTimeoutSeconds(String topic) {
		// TODO
		return 2;
	}

	@Override
	public Codec getCodecByType(String codecType) {
		Meta meta = m_meta.get();
		return meta.findCodec(codecType);
	}

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		try {
			return m_manager.getMetaProxy().tryAcquireConsumerLease(tpg, sessionId);
		} catch (RuntimeException e) {
			e.printStackTrace();
			// TODO
			throw e;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		try {
			return m_manager.getMetaProxy().tryRenewConsumerLease(tpg, lease, sessionId);
		} catch (RuntimeException e) {
			e.printStackTrace();
			// TODO
			throw e;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId) {
		try {
			return m_manager.getMetaProxy().tryRenewBrokerLease(topic, partition, lease, sessionId);
		} catch (RuntimeException e) {
			e.printStackTrace();
			// TODO
			throw e;
		}
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId) {
		try {
			return m_manager.getMetaProxy().tryAcquireBrokerLease(topic, partition, sessionId);
		} catch (RuntimeException e) {
			e.printStackTrace();
			// TODO
			throw e;
		}
	}

	@Override
	public void initialize() throws InitializationException {
		refreshMeta(m_manager.getMeta());
		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
					refreshMeta(m_manager.getMeta());
				} catch (Exception e) {
					// TODO
					e.printStackTrace();
				}
			}

		}, REFRESH_PERIOD_MINUTES, REFRESH_PERIOD_MINUTES, TimeUnit.MINUTES);
	}
	
	@Override
	public String findAvroSchemaRegistryUrl() {
		Codec avroCodec = m_meta.get().findCodec("avro");
		// TODO validate avro codec and extract "schema.registry.url" to constant
		return avroCodec.getProperties().get("schema.registry.url").getValue();
	}

}
