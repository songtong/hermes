package com.ctrip.hermes.collector.collector;

import org.apache.http.HttpResponse;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordContent;
import com.ctrip.hermes.collector.record.RecordType;

/**
 * @author tenglinxiao
 *
 */

@Component
public class CatHttpCollector extends HttpCollector {

	@Override
	public <T> Record<T> handleResponse(CollectorContext context, HttpResponse response) {
		// TODO Auto-generated method stub
		return null;
	}
	

	@Override
	public boolean retry(CollectorContext context, Record<?> record) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public static class CatHttpContext extends HttpCollectorContext {

		public CatHttpContext(HttpDatasource datasource, RecordType type) {
			super(datasource, type);
		}
		
	}
	
}
