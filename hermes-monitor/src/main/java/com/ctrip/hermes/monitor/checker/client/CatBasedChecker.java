package com.ctrip.hermes.monitor.checker.client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.client.fluent.Request;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.config.MonitorConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class CatBasedChecker implements Checker {

	private static final String CAT_DATE_PATTERN = "yyyyMMddkk";

	@Autowired
	protected MonitorConfig m_config;

	public static class Timespan {
		private Date m_startHour;

		private List<Integer> m_minutes = new LinkedList<>();

		public Date getStartHour() {
			return m_startHour;
		}

		public void setStartHour(Date startHour) {
			m_startHour = startHour;
		}

		public List<Integer> getMinutes() {
			return m_minutes;
		}

		public void addMinute(int minute) {
			m_minutes.add(minute);
		}

	}

	protected Timespan calTimespan(Date toDate, int minutesBefore) {
		if (minutesBefore > 60 || minutesBefore <= 0) {
			throw new IllegalArgumentException(String.format("MinutesBefore invalid(toDate=%s, minutesBefore=%s).",
			      toDate, minutesBefore));
		}

		Timespan timespan = new Timespan();

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);

		int minute = calendar.get(Calendar.MINUTE);

		if (minute == 0) {
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			timespan.setStartHour(calendar.getTime());

			for (int i = 0; i < minutesBefore; i++) {
				timespan.addMinute(59 - i);
			}
		} else {
			if (minute >= minutesBefore) {
				timespan.setStartHour(calendar.getTime());

				for (int i = 1; i <= minutesBefore; i++) {
					timespan.addMinute(minute - i);
				}
			} else {
				throw new IllegalArgumentException(String.format(
				      "There is not enough data for query(toDate=%s, minuteBefore=%s).", toDate, minutesBefore));
			}
		}

		return timespan;
	}

	protected String formatToCatUrlTime(Date date) {
		return new SimpleDateFormat(CAT_DATE_PATTERN).format(date);
	}

	protected String curl(String url, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
		try {
			return Request.Get(url)//
			      .connectTimeout(connectTimeoutMillis)//
			      .socketTimeout(readTimeoutMillis)//
			      .execute()//
			      .returnContent()//
			      .asString();
		} catch (IOException e) {
			throw new IOException(String.format("Failed to fetch data from url %s", url), e);
		}
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		CheckerResult result = new CheckerResult();

		try {
			Timespan timespan = calTimespan(toDate, minutesBefore);

			doCheck(timespan, result);
		} catch (Exception e) {
			result.setRunSuccess(false);
			result.setErrorMessage(e.getMessage());
			result.setException(e);
		}

		return result;
	}

	protected abstract void doCheck(Timespan timespan, CheckerResult result) throws Exception;

}