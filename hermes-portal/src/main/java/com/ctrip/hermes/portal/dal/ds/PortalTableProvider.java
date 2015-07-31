package com.ctrip.hermes.portal.dal.ds;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.dal.jdbc.QueryDef;
import org.unidal.dal.jdbc.QueryEngine;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.mapping.TableProvider;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaservice.service.PortalMetaService;

public class PortalTableProvider implements TableProvider {

	@Deprecated
	private PortalMetaService m_metaService;

	private final AtomicBoolean m_inited = new AtomicBoolean(false);

	@Override
	public String getDataSourceName(Map<String, Object> hints, String logicalTableName) {
		QueryDef def = (QueryDef) hints.get(QueryEngine.HINT_QUERY);
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		return findDataSourceName(def, tpAware);
	}

	@Override
	public String getPhysicalTableName(Map<String, Object> hints, String logicalTableName) {
		TopicPartitionAware tpAware = (TopicPartitionAware) hints.get(QueryEngine.HINT_DATA_OBJECT);

		String fmt = null;
		String db = toDbName(tpAware.getTopic(), tpAware.getPartition());
		switch (logicalTableName) {
		case "message-priority":
			fmt = "%s_message_%s";
			TopicPartitionPriorityAware tppAware = (TopicPartitionPriorityAware) tpAware;
			return String.format(fmt, db, tppAware.getPriority());

		case "resend-group-id":
			fmt = "%s_resend_%s";
			TopicPartitionPriorityGroupAware tpgAware = (TopicPartitionPriorityGroupAware) tpAware;
			return String.format(fmt, db, tpgAware.getGroupId());

		case "offset-message":
			fmt = "%s_offset_message";
			return String.format(fmt, db);

		case "offset-resend":
			fmt = "%s_offset_resend";
			return String.format(fmt, db);

		case "dead-letter":
			fmt = "%s_dead_letter";
			return String.format(fmt, db);

		default:
			throw new IllegalArgumentException(String.format("Unknown physical table for %s %s", hints, logicalTableName));
		}

	}

	private String toDbName(String topic, int partition) {
		String fmt = "%s_%s";
		return String.format(fmt, getMetaService().findTopicByName(topic).getId(), partition);
	}

	private String findDataSourceName(QueryDef def, TopicPartitionAware tpAware) {
		QueryType queryType = def.getType();

		Partition p = getMetaService().findPartition(tpAware.getTopic(), tpAware.getPartition());

		switch (queryType) {
		case INSERT:
		case DELETE:
		case UPDATE:
			return p.getWriteDatasource();
		case SELECT:
			return p.getReadDatasource();

		default:
			throw new IllegalArgumentException(String.format("Unknown query type '%s'", queryType));
		}
	}

	private PortalMetaService getMetaService() {
		if (!m_inited.get()) {
			synchronized (m_inited) {
				if (!m_inited.get()) {
					m_metaService = PlexusComponentLocator.lookup(PortalMetaService.class);
					m_inited.set(true);
				}
			}
		}
		return m_metaService;
	}
}
