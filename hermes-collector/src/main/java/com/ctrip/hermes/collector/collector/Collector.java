package com.ctrip.hermes.collector.collector;

import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordContent;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.utils.JsonSerializer;


/**
 * @author tenglinxiao
 */
public interface Collector {
	// Collect data.
	public <T> Record<T> collect(CollectorContext context) throws Exception;
	
	public boolean retry(CollectorContext context, Record<?> record);
	
	public abstract class CollectorContext {
		private RecordType m_type;
		private int m_retries = 1;
		
		public CollectorContext(RecordType type) {
			this.m_type = type;
		}

		public RecordType getType() {
			return m_type;
		}

		public void setType(RecordType type) {
			this.m_type = type;
		}

		public int getRetries() {
			return m_retries;
		}

		public void setRetries(int retries) {
			this.m_retries = retries;
		}
		
		public boolean retry() {
			return this.m_retries-- > 0;
		}
		
		public String toString() {
			try {
				return JsonSerializer.getInstance().serialize(this, false).toString();
			} catch (SerializeException e) {
				e.printStackTrace();
				return null;
			}
		}
		
	}
}
