package com.ctrip.hermes.collector.collector;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;


/**
 * @author tenglinxiao
 *
 */
@Component
public class MysqlDbCollector extends DbCollector {

	@Override
	public <T> Record<T> collect(CollectorContext context) throws Exception {
		return null;
	}
	
	public static class MysqlDbContext extends DbCollectorContext {

		public MysqlDbContext(RecordType type) {
			super(type);
		}
	}
}
