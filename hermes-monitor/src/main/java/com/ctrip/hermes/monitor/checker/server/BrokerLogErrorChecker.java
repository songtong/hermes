package com.ctrip.hermes.monitor.checker.server;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.service.ESMonitorService;

//@Component(value = BrokerLogErrorChecker.ID)
public class BrokerLogErrorChecker implements Checker {
	public static final String ID = "BrokerLogErrorChecker";

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
		CheckerResult r = new CheckerResult();
		if (m_config.isMonitorCheckerEnable()) {
			long from = toDate.getTime() - TimeUnit.MINUTES.toMillis(minutesBefore);

			try {
				Map<String, Long> map = m_es.queryBrokerErrorCount(new Date(from), toDate);
				for (Entry<String, Long> entry : map.entrySet()) {
					if (entry.getValue() >= m_config.getBrokerErrorThreshold()) {
						r.addMonitorEvent(new BrokerErrorEvent(entry.getKey(), entry.getValue()));
					}
				}
				r.setRunSuccess(true);
			} catch (Exception e) {
				r.setErrorMessage("Query broker log error failed.");
				r.setRunSuccess(false);
				r.setException(e);
			}
		}
		return r;
	}
}
