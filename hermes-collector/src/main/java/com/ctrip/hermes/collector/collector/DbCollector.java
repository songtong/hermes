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
@Component
public class DbCollector implements Collector {

	@Override
	public <T> Record<T> collect(CollectorContext context) {
		return null;
	}

	@Override
	public boolean retry(CollectorContext context, Record<?> record) {
		// TODO Auto-generated method stub
		return false;
	}
	
	protected static class DbCollectorContext extends CollectorContext {
		private DataSource datasource;

		public DbCollectorContext(RecordType type) {
			super(type);
		}

		public DataSource getDatasource() {
			return datasource;
		}

		public void setDatasource(DataSource datasource) {
			this.datasource = datasource;
		}
	}

}
