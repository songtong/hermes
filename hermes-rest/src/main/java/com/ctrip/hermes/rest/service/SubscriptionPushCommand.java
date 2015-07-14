package com.ctrip.hermes.rest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;

import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

public class SubscriptionPushCommand extends HystrixCommand<HttpResponse> {

	private HttpClient client;

	private RequestConfig config;

	private ConsumerMessage<RawMessage> msg;

	private String url;

	public SubscriptionPushCommand(HttpClient client, RequestConfig config, long subId, ConsumerMessage<RawMessage> msg,
	      String url) {
		super(HystrixCommandGroupKey.Factory.asKey(String.valueOf(subId)));
		this.client = client;
		this.config = config;
		this.msg = msg;
		this.url = url;
	}

	@Override
	protected HttpResponse run() throws ClientProtocolException, IOException {
		HttpPost post = new HttpPost(url);
		HttpResponse response = null;
		try {
			post.setConfig(config);
			ByteArrayInputStream stream = new ByteArrayInputStream(msg.getBody().getEncodedMessage());
			post.addHeader("X-Hermes-Topic", msg.getTopic());
			post.addHeader("X-Hermes-Ref-Key", msg.getRefKey());
			Iterator<String> propertyNames = msg.getPropertyNames();
			if (propertyNames.hasNext()) {
				StringBuilder sb = new StringBuilder();
				while (propertyNames.hasNext()) {
					String key = propertyNames.next();
					String value = msg.getProperty(key);
					sb.append(key).append('=').append(value);
				}
				post.addHeader("X-Hermes-Message-Property", sb.toString());
			}
			post.setEntity(new InputStreamEntity(stream, ContentType.APPLICATION_OCTET_STREAM));
			// post.setEntity(new StringEntity(new String(msg.getBody().getEncodedMessage()), ContentType.TEXT_PLAIN));
			response = client.execute(post);
		} finally {
			post.reset();
		}
		return response;
	}

	@Override
	protected HttpResponse getFallback() {
		return new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 500, "HystrixCommand fallback"));
	}

}
