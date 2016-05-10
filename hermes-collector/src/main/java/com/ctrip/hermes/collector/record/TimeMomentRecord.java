package com.ctrip.hermes.collector.record;

import java.util.Date;

/**
 * @author tenglinxiao
 * @param <T>
 *
 * */
public final class TimeMomentRecord<T> extends Record<T> implements TimeMoment{
	private Date m_moment;
	
	private TimeMomentRecord(RecordType type, Date moment) {
		super(type);
		this.m_moment =  moment;
	}

	@Override
	public void setMoment(Date moment) {
		this.m_moment = moment;
	}
	
	public Date getMoment() {
		return this.m_moment;
	}

	@Override
	public long getTimestamp() {
		return this.m_moment.getTime();
	}
	
	public static <T> TimeMomentRecord<T> newTimeMomentCommand(RecordType type, Date moment) {
		return new TimeMomentRecord<T>(type, moment);
	}
	
	public static <T> TimeMomentRecord<T> newTimeMomentCommand(RecordType type) {
		return new TimeMomentRecord<T>(type, null);
	}

}
