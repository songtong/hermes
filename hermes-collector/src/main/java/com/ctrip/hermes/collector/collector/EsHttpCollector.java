package com.ctrip.hermes.collector.collector;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.EsQueryContextBuilder.EsQueryContext;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder.EsQueryType;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.record.TimeMomentRecord;
import com.ctrip.hermes.collector.record.TimeWindowRecord;
import com.ctrip.hermes.collector.utils.TypeConverter;

/**
 * @author tenglinxiao
 *
 */

@Component
public class EsHttpCollector extends HttpCollector {
	private static Logger logger = LoggerFactory.getLogger(EsHttpCollector.class);
	private static ObjectMapper mapper = new ObjectMapper();  
	
	@SuppressWarnings("unchecked")
	public Record<JsonNode> handleResponse(CollectorContext context, HttpResponse response) {
		try {
			EsHttpCollectorContext ctx = (EsHttpCollectorContext)context;
			Record<JsonNode> record = context.getType().newRecord();
			if (record instanceof TimeWindowRecord) {
				TimeWindowRecord<JsonNode> timeWindowRecord = (TimeWindowRecord<JsonNode>)record;
				timeWindowRecord.setStartDate(TypeConverter.toDate(ctx.getQueryContext().getFrom()));		
				timeWindowRecord.setEndDate(TypeConverter.toDate(ctx.getQueryContext().getTo()));
			} else {
				TimeMomentRecord<JsonNode> timeMomentRecord = (TimeMomentRecord<JsonNode>)record;
				timeMomentRecord.setMoment(TypeConverter.toDate(ctx.getQueryContext().getFrom()));
			}
			// Always json data.
			JsonNode data = mapper.readTree(response.getEntity().getContent());
			record.setData(data);
			if (retry(context, record)) {
				logger.error(String.format("Failed request with ES context [%s] and received data [%s] ", ctx, data));
				record = collect(context);
			}
			return record;
		} catch (Exception e) {
			logger.error("Failed to parse response for es http request!", e);
			return null;
		} finally {
			logger.info("Issued request on es with query context: " + ((EsHttpCollectorContext)context).getQueryContext().getQuery());
		}
	}
	
	public boolean retry(CollectorContext context, Record<?> record) {
		EsHttpCollectorContext ctx = (EsHttpCollectorContext)context;
		if (ctx.getStatus().is2xxSuccessful()) {
			return false;
		}

		return ctx.retry();
	}
	
	public static class EsHttpCollectorContext extends HttpCollectorContext {
		private EsQueryContext m_queryContext;

		public EsHttpCollectorContext(HttpDatasource datasource, RecordType type) {
			super(datasource, type);
			setHeaders(new Header[]{new BasicHeader("Content-Type", "application/json")});
			setMethod(HttpMethod.POST);
		}

		public EsQueryContext getQueryContext() {
			return m_queryContext;
		}

		public void setQueryContext(EsQueryContext queryContext) {
			m_queryContext = queryContext;
		}

		public static CollectorContext newContext(EsDatasource datasource, String url, EsQueryContext queryContext, RecordType type) {
			EsHttpCollectorContext context = new EsHttpCollectorContext(datasource, type);
			
			// If url is not specified, then add indices names as the path.
			if (url == null) {
				url = StringUtils.join(queryContext.getIndices(), ",");
				
				// If the query type is QUERY, add the suffix to the path.
				if (queryContext.getQueryType() == EsQueryType.QUERY) {
					url += "/_search";
				}
			}

			context.setUrl(url);
			context.setQueryContext(queryContext);

			ObjectNode request = mapper.createObjectNode();
			
			// Set must-have access-token
			request.put("access_token", datasource.getToken());
			request.put("request_body", queryContext.getQuery());
			
			try {
				context.setData(new StringEntity(request.toString()));
			} catch (UnsupportedEncodingException e) {
				logger.error("Failed to set context data for es http request!", e);
			}
			return context;
		}	
	}
	
}

