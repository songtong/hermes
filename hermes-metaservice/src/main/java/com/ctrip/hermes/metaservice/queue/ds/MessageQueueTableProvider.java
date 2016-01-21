package com.ctrip.hermes.metaservice.queue.ds;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.MetaService;

public class MessageQueueTableProvider implements TableProvider {
	private static final String DEAD_LETTER_TABLE = "dead-letter";

	private static final String RESEND_OFFSET_TABLE = "offset-resend";

	private static final String MESSAGE_OFFSET_TABLE = "offset-message";

	private static final String RESEND_MESSAGE_TABLE = "resend-group-id";

	private static final String MESSAGE_TABLE = "message-priority";

	private static final Logger log = LoggerFactory.getLogger(MessageQueueTableProvider.class);

	private AtomicReference<Meta> m_meta = new AtomicReference<Meta>();

	@Override
	public String getDataSourceName(Map<String, Object> hints, String logicalTableName) {
		QueryDef def = (QueryDef) hints.get(QueryEngine.HINT_QUERY);
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		return findDataSourceName(def, tpAware, logicalTableName);
	}

	@Override
	public String getPhysicalTableName(Map<String, Object> hints, String logicalTableName) {
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		String fmt = null;
		String db = toDbName(tpAware.getTopic(), tpAware.getPartition());
		switch (logicalTableName) {
		case MESSAGE_TABLE:
			fmt = "%s_message_%s";
			TopicPartitionPriorityAware tppAware = (TopicPartitionPriorityAware) tpAware;
			return String.format(fmt, db, tppAware.getPriority());

		case RESEND_MESSAGE_TABLE:
			fmt = "%s_resend_%s";
			TopicPartitionPriorityGroupAware tpgAware = (TopicPartitionPriorityGroupAware) tpAware;
			return String.format(fmt, db, tpgAware.getGroupId());

		case MESSAGE_OFFSET_TABLE:
			fmt = "%s_offset_message";
			return String.format(fmt, db);

		case RESEND_OFFSET_TABLE:
			fmt = "%s_offset_resend";
			return String.format(fmt, db);

		case DEAD_LETTER_TABLE:
			fmt = "%s_dead_letter";
			return String.format(fmt, db);

		default:
			throw new IllegalArgumentException(String.format("Unknown physical table for %s %s", hints, logicalTableName));
		}

	}

	private String toDbName(String topic, int partition) {
		String fmt = "%s_%s";
		return String.format(fmt, findTopic(topic).getId(), partition);
	}

	private String findDataSourceName(QueryDef def, TopicPartitionAware tpAware, String logicalTableName) {
		QueryType queryType = def.getType();

		Partition p = findPartition(tpAware.getTopic(), tpAware.getPartition());

		switch (queryType) {
		case INSERT:
			return MESSAGE_TABLE.equals(logicalTableName) ? p.getWriteDatasource() : p.getReadDatasource();
		case SELECT:
		case UPDATE:
		case DELETE:
			return p.getReadDatasource();

		default:
			throw new IllegalArgumentException(String.format("Unknown query type '%s'", queryType));
		}
	}

	private synchronized void updateMeta() {
		try {
			m_meta.set(PlexusComponentLocator.lookup(MetaService.class).refreshMeta());
		} catch (DalException e) {
			log.error("Couldn't find latest meta-info from meta-db.", e);
		}
	}

	private Topic findTopic(String topicName) {
		if (m_meta.get() == null) {
			synchronized (m_meta) {
				if (m_meta.get() == null) {
					updateMeta();
				}
			}
		}

		Topic topic = null;
		if (m_meta.get() != null) {
			topic = m_meta.get().findTopic(topicName);
			if (topic == null) {
				updateMeta();
				topic = m_meta.get().findTopic(topicName);
			}
		}

		if (topic == null) {
			throw new RuntimeException("Topic not found: " + topicName);
		}

		return topic;
	}

	private Partition findPartition(String topicName, int partitionId) {
		Partition partition = null;
		Topic topic = findTopic(topicName);
		if (topic != null) {
			partition = topic.findPartition(partitionId);
			if (partition == null) {
				updateMeta();
				partition = findTopic(topicName).findPartition(partitionId);
			}
		}

		if (partition == null) {
			throw new RuntimeException(String.format("Partition %s of topic %s not found.", partitionId, topicName));
		}

		return partition;
	}
}
