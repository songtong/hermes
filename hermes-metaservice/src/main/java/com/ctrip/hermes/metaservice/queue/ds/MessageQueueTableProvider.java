package com.ctrip.hermes.metaservice.queue.ds;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.Partition;

@Named
public class MessageQueueTableProvider implements TableProvider {
	private static final String DEAD_LETTER_TABLE = "dead-letter";

	private static final String RESEND_OFFSET_TABLE = "offset-resend";

	private static final String MESSAGE_OFFSET_TABLE = "offset-message";

	private static final String RESEND_MESSAGE_TABLE = "resend-group-id";

	private static final String MESSAGE_TABLE = "message-priority";

	private static final Logger log = LoggerFactory.getLogger(MessageQueueTableProvider.class);

	@Inject
	private CachedTopicDao m_topicDao;

	@Inject
	private CachedPartitionDao m_partitionDao;

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
		return String.format("%s_%s", getTopicId(topic), partition);
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

	private long getTopicId(String topicName) {
		try {
			return m_topicDao.findByName(topicName).getId();
		} catch (DalException e) {
			throw new RuntimeException(e);
		}
	}

	private Partition findPartition(String topicName, int partitionId) {
		try {
			List<Partition> partitions = m_partitionDao.findByTopic(getTopicId(topicName));
			for (Partition p : partitions) {
				if (p.getId() == partitionId) {
					return p;
				}
			}
		} catch (DalException e) {
			log.error("Find partition [{}:{}] faile.d", topicName, partitionId, e);
			throw new RuntimeException(String.format("Find partition [%s:%s] faile.d", topicName, partitionId), e);
		}
		throw new RuntimeException(String.format("Find partition [%s:%s] faile.d", topicName, partitionId));
	}
}
