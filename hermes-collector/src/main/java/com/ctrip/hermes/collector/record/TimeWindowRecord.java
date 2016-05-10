package com.ctrip.hermes.collector.record;

import java.util.Date;

/**
 * @author tenglinxiao
 */
public final class TimeWindowRecord<T> extends Record<T> implements TimeWindow {
	private Date m_startDate;
	
	private Date m_endDate;
	
	private TimeWindowRecord(RecordType type, Date startDate, Date endDate) {
		super(type);
		this.m_startDate = startDate;
		this.m_endDate = endDate;
	}

	@Override
	public void setStartDate(Date startDate) {
		this.m_startDate = startDate;
	}

	public Date getStartDate() {
		return m_startDate;
	}

	public Date getEndDate() {
		return m_endDate;
	}

	@Override
	public long getTimestamp() {
		return m_startDate.getTime();
	}
	
	@Override
	public void setEndDate(Date endDate) {
		this.m_endDate = endDate;
	}
	
	public static <T> TimeWindowRecord<T> newTimeWindowCommand(RecordType type, Date startDate, Date endDate) {
		return new TimeWindowRecord<T>(type, startDate, endDate);
	}
	
	public static <T> TimeWindowRecord<T> newTimeWindowCommand(RecordType type) {
		return new TimeWindowRecord<T>(type, null, null);
	}

}
