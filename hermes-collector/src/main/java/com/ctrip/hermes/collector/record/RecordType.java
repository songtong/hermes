package com.ctrip.hermes.collector.record;

import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tenglinxiao
 *
 */
public enum RecordType {
	// Metaserver relatives.
	METASERVER_ERROR(RecordCategory.SERVICE, "error", true),

	// Broker relatives.
	BROKER_ERROR(RecordCategory.SERVICE, "error", true),
	
	TOPIC_FLOW_DB(RecordCategory.TOPIC_FLOW_DB, "topic-flow-db", true),
	
	// Topic relatives.
	TOPIC_FLOW(RecordCategory.TOPIC_FLOW, "topic-flow", true),
	TOPIC_FLOW_DAILY(RecordCategory.TOPIC_FLOW, "topic-flow-daily", true),
	
	//Report
	TOPIC_FLOW_DAILY_REPORT(RecordCategory.REPORT, "topic-flow-daily-report", true),
	
	PRODUCE_LATENCY(RecordCategory.STATISTICS, "produce-latency-statistics", true),
	COMANND_DROP(RecordCategory.STATISTICS, "command-drop", true),
	
	MAX_DATE(null, "max-date", true);
	
	private static String ID_FORMAT = "%d_%d";
	private RecordCategory m_category;
	private String m_name;
	private boolean m_isTimeWindow;
	private AtomicLong m_idGenerator = new AtomicLong(0);
	private long m_serviceUpTime = Calendar.getInstance().getTimeInMillis(); 
	
	private RecordType(RecordCategory category, String name, boolean isTimeWindow) {
		this.m_category = category;
		this.m_name = name;
		this.m_isTimeWindow = isTimeWindow;
	}
	
	public String getName() {
		return m_name;
	}
	
	public RecordCategory getCategory() {
		return m_category;
	}
	
	public boolean isTimeWindow() {
		return m_isTimeWindow;
	}
	
	public String nextId() {
		return String.format(ID_FORMAT, m_serviceUpTime, m_idGenerator.incrementAndGet());
	}
	
	public <T> Record<T> newRecord() {
		if (this.m_isTimeWindow) {
			return TimeWindowRecord.newTimeWindowRecord(this);
		}
		return TimeMomentRecord.newTimeMomentRecord(this);
	}
	
	public static RecordType findByCategoryType(String category, String name) {
		for (RecordType type : RecordType.values()) {
			if (type.getCategory().name().equals(category) && type.getName().equals(name)) {
				return type;
			}
		}
		return null;
	}
	
	public String toString() {
		return this.m_name;
	}
	
	public static enum RecordCategory {
		TOPIC_FLOW("topic-flow"), TOPIC_FLOW_DB("topic-flow-db"), SERVICE("service"), STATISTICS("statistics"), REPORT("report");
		
		private String m_name;
		
		private RecordCategory(String name) {
			this.m_name = name;
		}
		
		public String getName() {
			return m_name;
		}
	}
}
