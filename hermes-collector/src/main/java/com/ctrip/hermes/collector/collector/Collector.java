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
	
	/**
	 * Collect data within each approach.
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public <T> Record<T> collect(CollectorContext context) throws Exception;

	/**
	 * Whether should use retry policy.
	 * @param context
	 * @param record
	 * @return 
	 */
	public boolean retry(CollectorContext context, Record<?> record);
	
	public abstract class CollectorContext {
		public static final int DEFAULT_RETRY_TIMES = 1;
		public static final long DEFAULT_RETRY_INTERVAL = 3000;
		private RecordType m_type;
		private int m_retries = DEFAULT_RETRY_TIMES;
		private long m_retryIntervalMillis = DEFAULT_RETRY_INTERVAL;
		private boolean m_isSucceed;
		private boolean m_isRetryMode;
		
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
		
		public boolean isSucceed() {
			return m_isSucceed;
		}

		public void setSucceed(boolean isSucceed) {
			m_isSucceed = isSucceed;
		}

		public boolean isRetryMode() {
			return m_isRetryMode;
		}

		public void setRetryMode(boolean isRetryMode) {
			m_isRetryMode = isRetryMode;
		}

		public long getRetryIntervalMillis() {
			return m_retryIntervalMillis;
		}

		public void setRetryIntervalMillis(long retryIntervalMillis) {
			m_retryIntervalMillis = retryIntervalMillis;
		}

		public boolean retry() {
			if (isRetryMode()) {
				return this.m_retries-- > 0;
			}
			return false;
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
