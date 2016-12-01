package com.ctrip.hermes.collector.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.collector.dal.report.Report;
import com.ctrip.hermes.collector.dal.report.ReportDao;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author tenglinxiao
 *
 */
@Component(MysqlStorage.MYSQL_STORAGE)
public class MysqlStorage extends Storage {
	public static final String MYSQL_STORAGE = "mysql";
	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlStorage.class);
	
	private ReportDao m_reportDao = PlexusComponentLocator.lookup(ReportDao.class);
	
	public boolean save(State state) {
		Report report = new Report();
		try {
			return m_reportDao.insert(report) == 1;
		} catch (DalException e) {
			LOGGER.error("Failed to save report: {}", report, e);
			return false;
		}
	}

}
