package com.ctrip.hermes.metaservice.monitor.dao;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	@Inject
	MonitorEventDao m_dao;

	@Override
	public void addMonitorEvent(MonitorEvent event) throws Exception {
		com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDao = event.toDBEntity();
		Date date = new Date();
		monitorEventDao.setEventType(event.getType().getCode());
		monitorEventDao.setCreateTime(date);
		monitorEventDao.setDataChangeLastTime(date);
		m_dao.insert(monitorEventDao);
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<MonitorEvent> findMonitorEvent(MonitorEventType type, long start, long end) {
		try {
			return (List<MonitorEvent>) CollectionUtil.collect(
			      m_dao.findByTypeWithinTimeRange(type.getCode(), start, end, MonitorEventEntity.READSET_FULL),
			      new Transformer() {
				      @Override
				      public Object transform(Object obj) {
					      com.ctrip.hermes.metaservice.model.MonitorEvent event = (com.ctrip.hermes.metaservice.model.MonitorEvent) obj;
					      return MonitorEventParser.parse(event);
				      }
			      });
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

}
