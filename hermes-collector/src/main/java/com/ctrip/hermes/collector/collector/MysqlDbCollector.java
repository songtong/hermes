package com.ctrip.hermes.collector.collector;

import com.ctrip.hermes.collector.record.RecordType;


/**
 * @author tenglinxiao
 *
 */
public class MysqlDbCollector extends DbCollector {
	public static class MysqlDbContext extends DbCollectorContext {

		public MysqlDbContext(RecordType type) {
			super(type);
			// TODO Auto-generated constructor stub
		}
		
	}
}
