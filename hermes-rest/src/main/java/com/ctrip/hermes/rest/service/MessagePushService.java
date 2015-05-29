package com.ctrip.hermes.rest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.RawMessage;

@Named
public class MessagePushService implements Initializable {

	@Inject
	private SubscribeRegistry m_subsRegistry;

	@Inject
	private ClientEnvironment m_env;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	@Override
	public void initialize() throws InitializationException {
		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		Properties globalConfig = m_env.getGlobalConfig();
		// TODO config
		b.setConnectTimeout(Integer.valueOf(globalConfig.getProperty("push.http.connect.timeout", "2000")));
		b.setSocketTimeout(Integer.valueOf(globalConfig.getProperty("push.http.read.timeout", "5000")));
		m_requestConfig = b.build();
	}

	public void start() {
		List<Subscription> subs = m_subsRegistry.getSubscriptions();

		for (Subscription sub : subs) {
			startPusher(sub);
		}
	}

	private void startPusher(Subscription sub) {
		final List<String> urls = sub.getPushHttpUrls();

		Consumer.getInstance().start(sub.getTopic(), sub.getGroupId(),
		      new BaseMessageListener<RawMessage>(sub.getGroupId()) {

			      @Override
			      protected void onMessage(ConsumerMessage<RawMessage> msg) {
				      for (String url : urls) {
					      try {
						      pushMessage(msg, url);
					      } catch (IOException e) {
						      // TODO
					      } catch (RuntimeException e) {

					      }
				      }
			      }

		      });
	}

	private HttpPost pushMessage(ConsumerMessage<RawMessage> msg, String url) throws IOException {
		System.out.println("push msg " + msg.getBody());
		HttpPost post = new HttpPost(url);

		post.setConfig(m_requestConfig);
		ByteArrayInputStream stream = new ByteArrayInputStream(msg.getBody().getEncodedMessage());
		post.setEntity(new InputStreamEntity(stream, ContentType.APPLICATION_OCTET_STREAM));

		m_httpClient.execute(post);

		return post;
	}
}
