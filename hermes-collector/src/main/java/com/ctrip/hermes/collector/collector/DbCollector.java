package com.ctrip.hermes.collector.collector;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordContent;
import com.ctrip.hermes.collector.record.RecordType;

/**
 * @author tenglinxiao
 *
 */

public abstract class DbCollector implements Collector {

	@Override
	public boolean retry(CollectorContext context, Record<?> record) {
		// NO retry policy for db ops.
		return false;
	}
	
	protected static class DbCollectorContext extends CollectorContext {
		private DataSource m_datasource;

		public DbCollectorContext(RecordType type) {
			super(type);
		}

		public DataSource getDatasource() {
			return m_datasource;
		}

		public void setDatasource(DataSource datasource) {
			this.m_datasource = datasource;
		}
	}

}
