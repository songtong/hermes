package com.ctrip.hermes.rest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Meter;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Subscription;

@Named
public class MessagePushService implements Initializable {

	private static final Logger m_logger = LoggerFactory.getLogger(MessagePushService.class);

	@Inject
	private ClientEnvironment m_env;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	private ExecutorService m_executor;

	@Inject
	private MetricsManager m_metricsManager;

	@Override
	public void initialize() throws InitializationException {
		m_httpClient = HttpClients.createDefault();
		m_executor = Executors.newCachedThreadPool(HermesThreadFactory.create("MessagePush", true));

		Builder b = RequestConfig.custom();
		Properties globalConfig = m_env.getGlobalConfig();
		// TODO config
		b.setConnectTimeout(Integer.valueOf(globalConfig.getProperty("gateway.subcription.connect.timeout", "2000")));
		b.setSocketTimeout(Integer.valueOf(globalConfig.getProperty("gateway.subscription.socket.timeout", "5000")));
		m_requestConfig = b.build();
	}

	public ConsumerHolder startPusher(Subscription sub) {
		final Meter success_meter = m_metricsManager.meter("push_success", sub.getTopic(), sub.getGroup(), sub
		      .getEndpoints().toString());

		final Meter failed_meter = m_metricsManager.meter("push_fail", sub.getTopic(), sub.getGroup(), sub.getEndpoints()
		      .toString());

		final String[] urls = sub.getEndpoints().split(",");

		final ConsumerHolder consumerHolder = Consumer.getInstance().start(sub.getTopic(), sub.getGroup(),
		      new BaseMessageListener<RawMessage>(sub.getGroup()) {

			      @Override
			      protected void onMessage(final ConsumerMessage<RawMessage> msg) {
				      for (final String url : urls) {
					      m_executor.submit(new Runnable() {
						      public void run() {
							      try {
								      pushMessage(msg, url);
								      success_meter.mark();
							      } catch (Exception e) {
								      m_logger.warn("Push message failed", e);
								      failed_meter.mark();
							      }
						      }
					      });
				      }
			      }

		      });
		return consumerHolder;
	}

	private HttpPost pushMessage(ConsumerMessage<RawMessage> msg, String url) throws IOException {
		HttpPost post = new HttpPost(url);

		post.setConfig(m_requestConfig);
		ByteArrayInputStream stream = new ByteArrayInputStream(msg.getBody().getEncodedMessage());
		post.setEntity(new InputStreamEntity(stream, ContentType.APPLICATION_OCTET_STREAM));

		m_httpClient.execute(post);

		return post;
	}
}
