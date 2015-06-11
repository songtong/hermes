package com.ctrip.hermes.rest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.ws.rs.core.Response;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;
import com.ctrip.hermes.core.message.payload.RawMessage;

@Named
public class MessagePushService implements Initializable {

	private static final Logger m_logger = LoggerFactory.getLogger(MessagePushService.class);

	@Inject
	private ClientEnvironment m_env;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	@Inject
	private MetricsManager m_metricsManager;

	@Override
	public void initialize() throws InitializationException {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(100);

		m_httpClient = HttpClients.custom().setConnectionManager(cm).build();

		Builder b = RequestConfig.custom();
		Properties globalConfig = m_env.getGlobalConfig();
		// TODO config
		b.setConnectTimeout(Integer.valueOf(globalConfig.getProperty("gateway.subcription.connect.timeout", "2000")));
		b.setSocketTimeout(Integer.valueOf(globalConfig.getProperty("gateway.subscription.socket.timeout", "5000")));
		m_requestConfig = b.build();
	}

	public ConsumerHolder startPusher(SubscriptionView sub) {
		final Meter success_meter = m_metricsManager.meter("push_success", sub.getTopic(), sub.getGroup(), sub
		      .getEndpoints().toString());

		final Meter failed_meter = m_metricsManager.meter("push_fail", sub.getTopic(), sub.getGroup(), sub.getEndpoints()
		      .toString());

		final Timer push_timer = m_metricsManager.timer("push_timer", sub.getTopic(), sub.getGroup(), sub.getEndpoints()
		      .toString());

		final String[] urls = sub.getEndpoints().split(",");

		final ConsumerHolder consumerHolder = Consumer.getInstance().start(sub.getTopic(), sub.getGroup(),
		      new BaseMessageListener<RawMessage>(sub.getGroup()) {

			      @Override
			      protected void onMessage(final ConsumerMessage<RawMessage> msg) {
				      while (msg.getStatus() != MessageStatus.SUCCESS) {
					      if (failed_meter.getOneMinuteRate() > 0.5) {
						      m_logger.warn("Suspend current push");
						      break;
					      }

					      for (final String url : urls) {
						      HttpResponse pushResponse = null;
						      try {
							      Context time = push_timer.time();
							      pushResponse = pushMessage(msg, url);
							      time.stop();
							      System.out.println("OneMinuteRate: " + push_timer.getOneMinuteRate());
							      if (pushResponse.getStatusLine().getStatusCode() == Response.Status.OK.getStatusCode()) {
								      msg.ack();
								      success_meter.mark();
								      return;
							      } else if (pushResponse.getStatusLine().getStatusCode() >= Response.Status.INTERNAL_SERVER_ERROR
							            .getStatusCode()) {
								      msg.nack();
								      failed_meter.mark();
								      return;
							      } else {
								      m_logger.warn("Push message failed, reason:{} msg:{} url:{}", pushResponse
								            .getStatusLine().getReasonPhrase(), msg.getBody(), url);
								      failed_meter.mark();
								      continue;
							      }
						      } catch (Exception e) {
							      m_logger.warn("Push message failed", e);
							      failed_meter.mark();
						      }
					      }
				      }
			      }
		      });
		return consumerHolder;
	}

	private HttpResponse pushMessage(ConsumerMessage<RawMessage> msg, String url) throws IOException {
		HttpPost post = new HttpPost(url);
		HttpResponse response = null;
		try {
			post.setConfig(m_requestConfig);
			ByteArrayInputStream stream = new ByteArrayInputStream(msg.getBody().getEncodedMessage());
			post.setEntity(new InputStreamEntity(stream, ContentType.APPLICATION_OCTET_STREAM));
			response = m_httpClient.execute(post);
		} finally {
			post.reset();
		}
		return response;
	}
}
