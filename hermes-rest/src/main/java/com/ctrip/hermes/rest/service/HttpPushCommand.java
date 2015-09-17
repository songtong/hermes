package com.ctrip.hermes.rest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.ws.rs.core.Response;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;

import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

public class HttpPushCommand extends HystrixCommand<HttpResponse> {

	private CloseableHttpClient client;

	private RequestConfig config;

	private ConsumerMessage<RawMessage> msg;

	private String url;

	private HttpClientContext context;

	public HttpPushCommand(CloseableHttpClient client, RequestConfig config, ConsumerMessage<RawMessage> msg, String url) {
		super(HystrixCommandGroupKey.Factory.asKey(HttpPushCommand.class.getSimpleName()));
		this.client = client;
		// this.client = HttpClients.createDefault();
		this.context = HttpClientContext.create();
		this.config = config;
		this.msg = msg;
		this.url = url;
	}

	@Override
	protected HttpResponse run() throws ClientProtocolException, IOException {
		HttpPost post = new HttpPost(url);
		CloseableHttpResponse response = null;
		try {
			post.setConfig(config);
			byte[] encodedMessage = msg.getBody().getEncodedMessage();
			ByteArrayInputStream stream = new ByteArrayInputStream(encodedMessage);
			post.addHeader("X-Hermes-Topic", msg.getTopic());
			post.addHeader("X-Hermes-Ref-Key", msg.getRefKey());
			Iterator<String> propertyNames = msg.getPropertyNames();
			if (propertyNames.hasNext()) {
				StringBuilder sb = new StringBuilder();
				while (propertyNames.hasNext()) {
					String key = propertyNames.next();
					String value = msg.getProperty(key);
					sb.append(key).append('=').append(value).append(',');
				}
				sb.deleteCharAt(sb.length() - 1);
				post.addHeader("X-Hermes-Message-Property", sb.toString());
			}
			post.setEntity(new InputStreamEntity(stream, encodedMessage.length, ContentType.APPLICATION_OCTET_STREAM));
			// post.setEntity(new StringEntity(new String(msg.getBody().getEncodedMessage()), ContentType.TEXT_PLAIN));
			response = client.execute(post, context);
		} finally {
			if (response != null) {
				response.close();
			}
		}

		if (response != null) {
			int statusCode = response.getStatusLine().getStatusCode();
			// System.out.println("Post to : " + url + " code: " + statusCode);
			if (statusCode != Response.Status.OK.getStatusCode()
			      && statusCode != Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
				throw new RequestFailedException(response);
			}
		}
		return response;
	}

	@Override
	protected HttpResponse getFallback() {
		Throwable failedExecutionException = this.getFailedExecutionException();
		if (failedExecutionException != null) {
			if (failedExecutionException instanceof RequestFailedException) {
				RequestFailedException requestFailedException = (RequestFailedException) failedExecutionException;
				HttpResponse response = requestFailedException.getPayload();
				return response;
			} else {
				return new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1,
				      Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), failedExecutionException.getMessage()));
			}
		} else {
			if (this.isResponseTimedOut()) {
				return new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1,
				      Response.Status.REQUEST_TIMEOUT.getStatusCode(), "HystrixCommand Timeout"));
			}
			return new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1,
			      Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), "HystrixCommand fallback"));
		}
	}

	private static class RequestFailedException extends RuntimeException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private HttpResponse response;

		public RequestFailedException(HttpResponse response) {
			this.response = response;
		}

		public HttpResponse getPayload() {
			return this.response;
		}
	}
}
