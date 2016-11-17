package com.ctrip.hermes.monitor.job.partition;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.MonitorEventDao;
import com.ctrip.hermes.admin.core.monitor.event.CheckerExceptionEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.job.partition.PartitionManagementJob.PartitionCheckerResult;

@Component
public class PartitionManagementScheduler {
	private static final Logger log = LoggerFactory.getLogger(PartitionManagementScheduler.class);

	@Autowired
	@Qualifier(value = PartitionManagementJob.ID)
	PartitionManagementJob m_job;

	@Autowired
	private MonitorConfig m_config;

	private MonitorEventDao m_monitorEventDao = PlexusComponentLocator.lookup(MonitorEventDao.class);

	@Scheduled(initialDelay = 0L, fixedDelay = 900000L)
	public void execute() {
		log.info("Checker suite start..");
		printStartInfo();
		List<PartitionCheckerResult> results = null;
		try {
			results = m_job.check();
			for (PartitionCheckerResult result : results) {
				saveMonitorEvents(result.getPartitionChangeListResult());
				saveMonitorEvents(result.getPartitionInfo());
				printEndInfo(result.getPartitionChangeListResult());
			}
		} catch (Exception e) {
			log.error("Exception occurred while runing partition management job. ", e);
		}
	}

	private String formatExceptionDetail(String errorMessage, Exception exception) {
		StringBuilder sb = new StringBuilder();
		if (errorMessage != null) {
			sb.append(errorMessage).append("\n");
		}

		if (exception != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			exception.printStackTrace(new PrintStream(baos));
			sb.append(baos.toString());
		}

		return sb.toString();
	}

	private void saveMonitorEvents(CheckerResult result) {
		if (result != null) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (result.isRunSuccess()) {
				if (!result.getMonitorEvents().isEmpty()) {
					for (MonitorEvent event : result.getMonitorEvents()) {
						event.setShouldNotify(m_config.isMonitorCheckerNotifyEnable());
						try {
							m_monitorEventDao.insert(event.toDBEntity());
						} catch (DalException e) {
							log.error("Saven monitor event failed.", e);
						}
					}
				}
			} else {
				Date current = new Date();
				CheckerExceptionEvent event = new CheckerExceptionEvent();
				event.setCheckerName(getClass().getName());
				event.setDate(sdf.format(current));
				event.setDetail(String.format("[%s]Partition management job %s throw exception.\n %s", sdf.format(current),
				      getClass().getName(), formatExceptionDetail(result.getErrorMessage(), result.getException())));
				try {
					m_monitorEventDao.insert(event.toDBEntity());
				} catch (DalException e) {
					log.error("Saven monitor event failed.", e);
				}
			}
		}
	}

	private void printEndInfo(CheckerResult result) {
		log.info("-----------------------------------------------------------------");
		if (result == null) {
			log.error("No checker result found, partition management job failed!");
		} else {
			if (result.isRunSuccess()) {
				log.info("Partition management job success, generate {} monitor events.", result.getMonitorEvents().size());
			} else {
				log.info("Partition management job failed(errorMessage={})!!!", result.getErrorMessage());
			}
		}
		log.info("-----------------------------------------------------------------");
	}

	private void printStartInfo() {
		log.info("-----------------------------------------------------------------");
		log.info("Starting partition management job ...");
		log.info("Partition management job class name: {}", getClass().getName());
		log.info("-----------------------------------------------------------------");
	}

}
