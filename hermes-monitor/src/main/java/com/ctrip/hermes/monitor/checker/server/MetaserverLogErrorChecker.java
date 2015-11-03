package com.ctrip.hermes.monitor.checker.server;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.metaservice.monitor.event.MetaServerErrorEvent;
import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.service.ESMonitorService;

@Component(value = MetaserverLogErrorChecker.ID)
public class MetaserverLogErrorChecker implements Checker {
	public static final String ID = "MetaserverLogErrorChecker";

	@Autowired
	private ESMonitorService m_es;

	@Autowired
	private MonitorConfig m_config;

	@Override
	public String name() {
		return ID;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		long to = toDate.getTime();
		long from = to - TimeUnit.MINUTES.toMillis(minutesBefore);

		CheckerResult r = new CheckerResult();

		try {
			Map<String, Long> map = m_es.queryMetaserverErrorCount(from, to);
			for (Entry<String, Long> entry : map.entrySet()) {
				if (entry.getValue() >= m_config.getMetaserverErrorThreshold()) {
					r.addMonitorEvent(new MetaServerErrorEvent(entry.getKey(), entry.getValue()));
				}
			}
			r.setRunSuccess(true);
		} catch (Exception e) {
			r.setErrorMessage("Query meta-server log error failed.");
			r.setRunSuccess(false);
			r.setException(e);
		}
		return r;
	}
}
