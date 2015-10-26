package com.ctrip.hermes.monitor.service;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.MonitorReport;
import com.ctrip.hermes.metaservice.model.MonitorReportDao;
import com.ctrip.hermes.metaservice.model.MonitorReportEntity;

@Service
public class MonitorReportService {

	private MonitorReportDao dao = PlexusComponentLocator.lookup(MonitorReportDao.class);

	public void insertOrUpdate(MonitorReport report) throws DalException {
		try {
			MonitorReport existingReport = dao.findExistingReport(report.getSource(), report.getCategory(),
			      report.getStart(), report.getEnd(), report.getHost(), MonitorReportEntity.READSET_FULL);
			report.setId(existingReport.getId());
			dao.updateByPK(report, MonitorReportEntity.UPDATESET_FULL);
		} catch (DalNotFoundException e) {
			dao.insert(report);
		}
	}
}
