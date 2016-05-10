package com.ctrip.hermes.collector.collector;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;

import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;


/**
 * @author tenglinxiao
 *
 */
public abstract class HttpCollector implements Collector{

	public <T> Record<T> collect(CollectorContext context) throws Exception {
		final HttpCollectorContext ctx = (HttpCollectorContext) context;
		
		Response response = null;
		switch(ctx.getMethod()) {
		case GET: 
			response = get(ctx);
			break;
		case POST: 
			response = post(ctx);
			break;
		default: throw new UnsupportedOperationException(String.format("Http request with method [%s] is not allowed!", ctx.getMethod()));
		}
		
		return response.handleResponse(new ResponseHandler<Record<T>>() {

			@Override
			public Record<T> handleResponse(HttpResponse response)
					throws ClientProtocolException, IOException {
				ctx.setStatus(HttpStatus.valueOf(response.getStatusLine().getStatusCode()));
				return HttpCollector.this.handleResponse(ctx, response);
			}
		});
	}
	
	public abstract <T> Record<T> handleResponse(CollectorContext context, HttpResponse response);
	
	private Response get(HttpCollectorContext context) throws ClientProtocolException, IOException {
		return Request.Get(context.getFullApi())
				.connectTimeout(context.getConnectTimeout())
			    .socketTimeout(context.getReadTimeOut())
			    .execute();
	}
	
	private Response post(HttpCollectorContext context) throws ClientProtocolException, IOException {
		return Request.Post(context.getFullApi())
				.setHeaders(context.getHeaders())
				.body(context.getData())
				.connectTimeout(context.getConnectTimeout())
			    .socketTimeout(context.getReadTimeOut())
				.execute();
	}
	
	protected static class HttpCollectorContext extends CollectorContext{
		private HttpDatasource m_datasource;
		private String url;
		private Header[] headers; 
		private HttpMethod m_method;
		private HttpEntity m_data;
		private int m_connectTimeout;
		private int m_readTimeOut;
		private HttpStatus m_status;
		
		public HttpCollectorContext(HttpDatasource datasource, RecordType type) {
			this(datasource, HttpMethod.GET, type);
		}
		
		public HttpCollectorContext(HttpDatasource datasource, HttpMethod method, RecordType type) {
			super(type);
			this.m_datasource = datasource;
			this.m_method = method;
		}
		
		public HttpDatasource getDatasource() {
			return m_datasource;
		}

		public void setDatasource(HttpDatasource datasource) {
			m_datasource = datasource;
		}

		public Header[] getHeaders() {
			return headers;
		}

		public void setHeaders(Header[] headers) {
			this.headers = headers;
		}

		public HttpMethod getMethod() {
			return m_method;
		}

		public void setMethod(HttpMethod method) {
			m_method = method;
		}

		@JsonIgnore
		public HttpEntity getData() {
			return m_data;
		}

		public void setData(HttpEntity data) {
			m_data = data;
		}

		public int getConnectTimeout() {
			return m_connectTimeout;
		}

		public void setConnectTimeout(int connectTimeout) {
			m_connectTimeout = connectTimeout;
		}

		public int getReadTimeOut() {
			return m_readTimeOut;
		}

		public void setReadTimeOut(int readTimeOut) {
			m_readTimeOut = readTimeOut;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}
		
		public HttpStatus getStatus() {
			return m_status;
		}

		public void setStatus(HttpStatus status) {
			m_status = status;
		}

		public String getFullApi() {
			String api = this.getDatasource().getApi();
			if (api.endsWith("/")) {
				return url.startsWith("/")? api + url.substring(1): api + url;
			} else {
				return url.startsWith("/")? api + url: api + "/" + url;
			}
		}
	}	
}
