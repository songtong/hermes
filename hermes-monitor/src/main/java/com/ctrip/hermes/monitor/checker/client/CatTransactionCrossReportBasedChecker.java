package com.ctrip.hermes.monitor.checker.client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.client.fluent.Request;

import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class CatTransactionCrossReportBasedChecker implements Checker {

	private static final String CAT_DATE_PATTERN = "yyyyMMddkk";

	private static final String CAT_TRANSACTION_CROSS_REPORT_URL_PATTERN = "http://cat.ctripcorp.com/cat/r/t?op=graphs&domain=All&date=%s&ip=All&type=%s&forceDownload=xml";

	private static final int CAT_CONNECT_TIMEOUT = 10 * 1000;

	private static final int CAT_READ_TIMEOUT = 30 * 1000;

	protected static class Timespan {
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
		if (minutesBefore > 60) {
			throw new IllegalArgumentException(String.format(
			      "Timespan can not larger than 60 minutes(toDate=%s, minutesBefore=%s).", toDate, minutesBefore));
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

	public CatTransactionCrossReportBasedChecker() {
		super();
	}

	protected String getCatTransactionCrossReport(Date startHour, String transactionType) throws IOException {
		String uri = String.format(CAT_TRANSACTION_CROSS_REPORT_URL_PATTERN,
		      new SimpleDateFormat(CAT_DATE_PATTERN).format(startHour), transactionType);

		try {
			return Request.Get(uri)//
			      .connectTimeout(CAT_CONNECT_TIMEOUT)//
			      .socketTimeout(CAT_READ_TIMEOUT)//
			      .execute()//
			      .returnContent()//
			      .asString();
		} catch (IOException e) {
			throw new IOException(String.format("Failed to fetch data from cat(uri=%s)", uri), e);
		}
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		CheckerResult result = new CheckerResult();

		try {
			Timespan timespan = calTimespan(toDate, minutesBefore);

			String transactionReportXml = getCatTransactionCrossReport(timespan.getStartHour(), getTransactionType());

			doCheck(transactionReportXml, timespan, result);
		} catch (Exception e) {
			result.setRunSuccess(false);
			result.setErrorMessage(e.getMessage());
			result.setException(e);
		}

		return result;
	}

	protected abstract String getTransactionType();

	protected abstract void doCheck(String transactionReportXml, Timespan timespan, CheckerResult result)
	      throws Exception;

}