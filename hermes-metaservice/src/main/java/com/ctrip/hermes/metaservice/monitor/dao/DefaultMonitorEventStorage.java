package com.ctrip.hermes.metaservice.monitor.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.metaservice.model.MonitorEventDao;
import com.ctrip.hermes.metaservice.model.MonitorEventEntity;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEventParser;

@Named(type = MonitorEventStorage.class)
public class DefaultMonitorEventStorage implements MonitorEventStorage {
	private static final Logger log = LoggerFactory.getLogger(DefaultMonitorEventStorage.class);

	private static final String MONITOR_EVENT_DS_NAME = "fxhermesmetadb";

	@Inject
	MonitorEventDao m_dao;

	@Inject
	TransactionManager m_transactionManager;

	@Override
	public void addMonitorEvent(MonitorEvent event) throws Exception {
		com.ctrip.hermes.metaservice.model.MonitorEvent entity = event.toDBEntity();
		m_dao.insert(entity);
	}

	@Override
	public List<MonitorEvent> findMonitorEvent(MonitorEventType type, long start, long end) {
		try {
			return MonitorEventParser.parse(//
			      m_dao.findByTypeWithinTimeRange(type.getCode(), start, end, MonitorEventEntity.READSET_FULL));
		} catch (Exception e) {
			log.error("Find monitor event from db failed. [{}, {}, {}]", type, start, end, e);
		}
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<MonitorEvent> findMonitorEvent(long start, long end) {
		try {
			return (List<MonitorEvent>) CollectionUtil.collect(
			      m_dao.findByTimeRange(start, end, MonitorEventEntity.READSET_FULL), new Transformer() {
				      @Override
				      public Object transform(Object obj) {
					      com.ctrip.hermes.metaservice.model.MonitorEvent event = (com.ctrip.hermes.metaservice.model.MonitorEvent) obj;
					      return MonitorEventParser.parse(event);
				      }
			      });
		} catch (Exception e) {
			log.error("Find monitor event from db failed. [{}, {}]", start, end, e);
		}
		return null;
	}

	@Override
	public List<MonitorEvent> fetchUnnotifiedMonitorEvent(boolean isForNotify) {
		return MonitorEventParser.parse(isForNotify ? findAndUpdateUnnotifiedEvents() : findUnnotifiedEvents());
	}

	private List<com.ctrip.hermes.metaservice.model.MonitorEvent> findAndUpdateUnnotifiedEvents() {
		boolean isSuccess = false;
		try {
			m_transactionManager.startTransaction(MONITOR_EVENT_DS_NAME);
			List<com.ctrip.hermes.metaservice.model.MonitorEvent> unnotifiedEvents = //
			m_dao.findUnnotifiedEvents(MonitorEventEntity.READSET_FULL);
			m_dao.updateNotifiedStatus(
			      unnotifiedEvents.toArray(new com.ctrip.hermes.metaservice.model.MonitorEvent[unnotifiedEvents.size()]),
			      MonitorEventEntity.UPDATESET_FULL);
			isSuccess = true;
			return unnotifiedEvents;
		} catch (Exception e) {
			log.error("Find and Update monitor event failed.", e);
			return null;
		} finally {
			if (isSuccess) {
				m_transactionManager.commitTransaction();
			} else {
				m_transactionManager.rollbackTransaction();
			}
		}
	}

	private List<com.ctrip.hermes.metaservice.model.MonitorEvent> findUnnotifiedEvents() {
		try {
			return m_dao.findUnnotifiedEvents(MonitorEventEntity.READSET_FULL);
		} catch (Exception e) {
			log.error("Find unnotified monitor event failed.", e);
		}
		return null;
	}

	@Override
	public List<com.ctrip.hermes.metaservice.model.MonitorEvent> findDBMonitorEvents(int pageCount, int pageOffset) {
		try {
			return m_dao.findEventsBatch(pageCount, pageOffset, MonitorEventEntity.READSET_FULL);
		} catch (DalException e) {
			log.error("Find monitor event failed.", e);
			return new ArrayList<com.ctrip.hermes.metaservice.model.MonitorEvent>();
		}
	}

	@Override
	public long totalPageCount(int pageCount) {
		try {
			return (long) Math.ceil(m_dao.totalCount(MonitorEventEntity.READSET_COUNT).getTotal() / (double) pageCount);
		} catch (DalException e) {
			return 0L;
		}
	}
}
