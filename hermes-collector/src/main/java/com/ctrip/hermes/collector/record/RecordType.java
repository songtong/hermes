package com.ctrip.hermes.collector.record;

import java.lang.reflect.Constructor;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;
import com.ctrip.hermes.collector.exception.RequiredException;
import com.ctrip.hermes.collector.recordcontent.ServiceRecordContent;
import com.ctrip.hermes.collector.recordcontent.StorageResultContent;
import com.ctrip.hermes.collector.recordcontent.TopicFlowContent;

/**
 * @author tenglinxiao
 *
 */
public enum RecordType {
	// Metaserver relatives.
	METASERVER_ERROR(RecordCategory.METASERVER, "error", true, ServiceRecordContent.class),

	// Broker relatives.
	BROKER_ERROR(RecordCategory.BROKER, "error", true, ServiceRecordContent.class),
	
	// Topic relatives.
	TOPIC_FLOW(RecordCategory.TOPIC, "topic-flow", true, TopicFlowContent.class),
	TOPIC_FLOW_DAILY(RecordCategory.TOPIC, "topic-flow-daily", true, TopicFlowContent.class),
	TOPIC_FLOW_MONTHLY(RecordCategory.TOPIC, "topic-flow-monthly", true, TopicFlowContent.class),
	
	//Report
	TOPIC_FLOW_DAILY_REPORT(RecordCategory.TOPIC,"topic-flow-daily-report",true,TopicFlowContent.class),
	
	// Storage relatives.
	STORAGE_RESULT(RecordCategory.STORAGE, "storage-result", false, StorageResultContent.class);
	
	private static String ID_FORMAT = "%d_%d";
	private RecordCategory m_category;
	private String m_name;
	private boolean m_isTimeWindow;
	private Class<? extends RecordContent> m_clz;
	private AtomicLong m_idGenerator = new AtomicLong(0);
	private long m_serviceUpTime = Calendar.getInstance().getTimeInMillis(); 
	
	private RecordType(RecordCategory category, String name, boolean isTimeWindow, Class<? extends RecordContent> clz) {
		this.m_category = category;
		this.m_name = name;
		this.m_isTimeWindow = isTimeWindow;
		this.m_clz = clz;
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
	
	public Class<? extends RecordContent> getClz() {
		return m_clz;
	}
	
	public String nextId() {
		return String.format(ID_FORMAT, m_serviceUpTime, m_idGenerator.incrementAndGet());
	}
	
	public RecordContent newContent() throws RequiredException {
		try {
			Constructor<? extends RecordContent> constructor = m_clz.getConstructor();
			if (constructor.isAccessible()) {
				constructor.setAccessible(true);
			}
			return constructor.newInstance();	
		} catch (Exception e) {
			throw new RequiredException(String.format("Default constructor must be offered for class [%s]!", m_clz.getName()), e);
		}
	}
	
	public <T> Record<T> newRecord() {
		if (this.m_isTimeWindow) {
			return TimeWindowRecord.newTimeWindowCommand(this);
		}
		return TimeMomentRecord.newTimeMomentCommand(this);
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
		METASERVER, BROKER, TOPIC, STORAGE;
	}
}
