package com.ctrip.hermes.monitor.checker.server;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.metaservice.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.service.ESMonitorService;

@Component(value = BrokerLogErrorChecker.ID)
public class BrokerLogErrorChecker implements Checker {
	public static final String ID = "BrokerLogErrorChecker";

	@Autowired
	private ESMonitorService m_es;

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
			Map<String, Long> map = m_es.queryBrokerErrorCount(from, to);
			for (Entry<String, Long> entry : map.entrySet()) {
				r.addMonitorEvent(new BrokerErrorEvent(entry.getKey(), entry.getValue()));
			}
			r.setRunSuccess(true);
		} catch (Exception e) {
			r.setErrorMessage("Query broker log error failed.");
			r.setRunSuccess(false);
			r.setException(e);
		}
		return r;
	}
}
