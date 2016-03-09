package com.ctrip.hermes.monitor.checker.meta;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ctrip.hermes.metaservice.monitor.event.MetaRequestErrorEvent.MetaRequestErrorType;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CatUrlBuilder;
import com.ctrip.hermes.monitor.checker.CheckerResult;

public abstract class AbstractMetaRequestErrorChecker extends CatBasedChecker {

	protected abstract List<String> getUrlChecklist();

	protected abstract int getTimeoutMillisecondLimit();

	protected abstract int getTimeoutCountLimit();

	protected abstract int getErrorCountLimit();

	protected abstract MonitorEvent generateEvent(String name, String meta, MetaRequestErrorType type, int count);

	@Override
	protected void doCheck(Timespan timespan, CheckerResult result) throws Exception {
		CatUrlBuilder cub = new CatUrlBuilder(m_config.getCatBaseUrl(), true) //
		      .domain(m_config.getHermesMetaserverCatDomain()).type("URL") //
		      .date(formatToCatUrlTime(timespan.getStartHour())).xml();

		for (String ip : getMetaServerList(m_config.getMetaserverListUrl())) {
			for (String url : getUrlChecklist()) {
				checkCatReport(timespan, result, cub, ip, url);
			}
		}

		result.setRunSuccess(true);
	}

	private void checkCatReport(Timespan timespan, CheckerResult result, CatUrlBuilder cub, String ip, String catName)
	      throws Exception {
		String xml = curl(cub.ip(ip).name(catName).build(), m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		Map<Integer, CatRangeEntity> ranges = extractCatNameRangesFromXml(xml, cub.getType()).get(catName);
		if (ranges != null) {
			int failCount = calRangeFailCount(ranges, timespan);
			int timeoutCount = calRangeTimeoutCount(ranges, timespan, getTimeoutMillisecondLimit());
			if (failCount > getErrorCountLimit()) {
				result.addMonitorEvent(generateEvent(catName, ip, MetaRequestErrorType.FAIL, failCount));
			}
			if (timeoutCount > getTimeoutCountLimit()) {
				result.addMonitorEvent(generateEvent(catName, ip, MetaRequestErrorType.TIMEOUT, timeoutCount));
			}
		}
	}

	private int calRangeFailCount(Map<Integer, CatRangeEntity> ranges, Timespan timespan) {
		int total = 0;
		for (Entry<Integer, CatRangeEntity> entry : ranges.entrySet()) {
			if (timespan.getMinutes().contains(entry.getKey())) {
				total += entry.getValue().getFails();
			}
		}
		return total;
	}

	private int calRangeTimeoutCount(Map<Integer, CatRangeEntity> ranges, Timespan timespan, int milliSecondLimit) {
		int total = 0;
		for (Entry<Integer, CatRangeEntity> entry : ranges.entrySet()) {
			if (timespan.getMinutes().contains(entry.getKey()) && entry.getValue().getAvg() >= milliSecondLimit) {
				total += entry.getValue().getCount();
			}
		}
		return total;
	}
}
