package com.ctrip.hermes.collector.collector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;

import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;


/**
 * @author tenglinxiao
 *
 */
public abstract class HttpCollector implements Collector{
	
	public <T> Record<T> collect(CollectorContext context) throws Exception {
		final HttpCollectorContext ctx = (HttpCollectorContext) context;
		
		if (context.isRetryMode()) {
			LockSupport.parkNanos(ctx.getRetryIntervalMillis() * 1000000);
		}
		
		InputStream input = null;
		Transaction requestTransaction = null;
		try {
			requestTransaction = Cat.newTransaction("HttpCollect", context instanceof EsHttpCollectorContext? "EsHttpCollect": "CatHttpCollect");
			switch(ctx.getMethod()) {
			case GET: 
				input = get(ctx);
				break;
			case POST: 
				input = post(ctx);
				break;
			default: throw new UnsupportedOperationException(String.format("Http request with method [%s] is not allowed!", ctx.getMethod()));
			}
			requestTransaction.setStatus(Message.SUCCESS);
		} finally {
			requestTransaction.complete();
		}
		
		if (input == null) {
			return null;
		}
		
		return this.handleResponse(context, input);
	}
	
	public boolean retry(CollectorContext context, Record<?> record) {
		EsHttpCollectorContext ctx = (EsHttpCollectorContext)context;
		if (ctx.getStatus().is2xxSuccessful()) {
			return false;
		}

		ctx.setRetryMode(true);
		return ctx.retry();
	}
	
	public abstract <T> Record<T> handleResponse(CollectorContext context, InputStream input);
	
	private InputStream get(HttpCollectorContext context) throws ClientProtocolException, IOException, URISyntaxException {
		HttpURLConnection connection = (HttpURLConnection)new URL(context.getFullApiWithParameters()).openConnection();
		connection.setConnectTimeout(context.getConnectTimeout());
		connection.setReadTimeout(context.getReadTimeOut());
		connection.connect();
		context.setStatus(HttpStatus.valueOf(connection.getResponseCode()));
		return connection.getInputStream();
	}
	
	private InputStream post(HttpCollectorContext context) throws ClientProtocolException, IOException, URISyntaxException {
		HttpURLConnection connection = (HttpURLConnection)new URL(context.getFullApiWithParameters()).openConnection();
		connection.setDoOutput(true);
		connection.setConnectTimeout(context.getConnectTimeout());
		connection.setReadTimeout(context.getReadTimeOut());
		ByteArrayOutputStream out = new ByteArrayOutputStream((int)context.getData().getContentLength());  
		InputStream input = context.getData().getContent();
		byte[] bytes = new byte[1024];
		int length = -1;
		while ((length = input.read(bytes)) != -1) {
			out.write(bytes, 0, length);
		}
		input.close();
		connection.getOutputStream().write(out.toByteArray());
		connection.connect();
		context.setStatus(HttpStatus.valueOf(connection.getResponseCode()));
		return connection.getInputStream();
	}
	
	protected static class HttpCollectorContext extends CollectorContext{
		private HttpDatasource m_datasource;
		private String url;
		private Header[] headers; 
		private HttpMethod m_method;
		private HttpEntity m_data;
		private List<NameValuePair> m_parameters;
		private int m_connectTimeout = 10000;
		private int m_readTimeOut = 10000;
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
		
		public List<NameValuePair> getParameters() {
			return m_parameters;
		}

		public void setParameters(List<NameValuePair> parameters) {
			m_parameters = parameters;
		}
		
		public void addParameter(String name, String value) {
			if (m_parameters == null) {
				m_parameters = new ArrayList<NameValuePair>();
			}
			m_parameters.add(new BasicNameValuePair(name, value));
		}

		public String getFullApi() {
			String api = this.getDatasource().getApi();
			if (api.endsWith("/")) {
				return url.startsWith("/")? api + url.substring(1): api + url;
			} else {
				return url.startsWith("/")? api + url: api + "/" + url;
			}
		}
		
		public String getFullApiWithParameters() throws URISyntaxException {
			String api = this.getFullApi();
			if (this.getParameters() != null) {
				URIBuilder builder = new URIBuilder(api);
				builder.addParameters(this.getParameters());
				api = builder.build().toString();
			}
			return api;
		}
	}	
}
