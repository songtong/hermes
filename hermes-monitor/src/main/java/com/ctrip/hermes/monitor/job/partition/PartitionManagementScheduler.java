package com.ctrip.hermes.monitor.job.partition;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.MonitorEventDao;
import com.ctrip.hermes.metaservice.monitor.event.CheckerExceptionEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.job.partition.PartitionManagementJob.PartitionCheckerResult;

@Component
public class PartitionManagementScheduler {
	private static final Logger log = LoggerFactory.getLogger(PartitionManagementScheduler.class);

	@Autowired
	@Qualifier(value = PartitionManagementJob.ID)
	PartitionManagementJob m_job;

	private MonitorEventDao m_monitorEventDao = PlexusComponentLocator.lookup(MonitorEventDao.class);

	@Scheduled(cron = "13 17 2 * * *")
	// @Scheduled(fixedDelay = 86400000L, initialDelay = 0L)
	public void execute() {
		printStartInfo();
		PartitionCheckerResult result = null;
		try {
			result = m_job.check();
			saveMonitorEvents(result.getPartitionChangeListResult());
			saveMonitorEvents(result.getPartitionInfo());
		} catch (Exception e) {
			log.error("Exception occurred while runing partition management job. ", e);
		}
		printEndInfo(result.getPartitionChangeListResult());
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