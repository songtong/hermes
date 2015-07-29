package com.ctrip.hermes.rest.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.Response;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Disposable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.log.BizEvent;
import com.ctrip.hermes.core.log.BizLogger;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.metrics.HermesMetricsRegistry;

@Named
public class SubscriptionPushService implements Initializable, Disposable {

	private static final Logger m_logger = LoggerFactory.getLogger(SubscriptionPushService.class);

	@Inject
	private BizLogger m_bizLogger;

	@Inject
	private ClientEnvironment m_env;

	private CloseableHttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	private Meter failedMeterGlobal;

	private Meter requestMeterGlobal;

	private Histogram requestSizeHistogramGlobal;

	private Timer pushTimerGlobal;

	private Map<String, Meter> failedMeterByTopic;

	private Map<String, Meter> requestMeterByTopic;

	private Map<String, Histogram> requestSizeHistogramByTopic;

	private Map<String, Timer> pushTimerByTopic;

	@Override
	public void dispose() {
		try {
			m_httpClient.close();
		} catch (IOException e) {
			m_logger.warn("Dispose SubscriptionPushService", e);
		}
	}

	private Timer getPushTimer(String topic) {
		if (!pushTimerByTopic.containsKey(topic)) {
			Timer pushTimer = HermesMetricsRegistry.getMetricRegistry().timer(
			      MetricRegistry.name(SubscriptionPushService.class, topic, "MessageSubscription"));
			pushTimerByTopic.put(topic, pushTimer);
		}
		return pushTimerByTopic.get(topic);
	}

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

		failedMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
		      MetricRegistry.name(SubscriptionPushService.class, "MessageSubscription", "Failed"));
		requestMeterGlobal = HermesMetricsRegistry.getMetricRegistry().meter(
		      MetricRegistry.name(SubscriptionPushService.class, "MessageSubscription", "Request"));
		requestSizeHistogramGlobal = HermesMetricsRegistry.getMetricRegistry().histogram(
		      MetricRegistry.name(SubscriptionPushService.class, "MessageSubscription", "BodySize"));
		pushTimerGlobal = HermesMetricsRegistry.getMetricRegistry().timer(
		      MetricRegistry.name(SubscriptionPushService.class, "MessageSubscription"));
		failedMeterByTopic = new HashMap<>();
		requestMeterByTopic = new HashMap<>();
		requestSizeHistogramByTopic = new HashMap<>();
		pushTimerByTopic = new HashMap<>();
	}

	public ConsumerHolder startPusher(final SubscriptionView sub) {
		final String[] urls = sub.getEndpoints().split(",");

		final ConsumerHolder consumerHolder = Consumer.getInstance().start(sub.getTopic(), sub.getGroup(),
		      new BaseMessageListener<RawMessage>() {

			      @Override
			      protected void onMessage(final ConsumerMessage<RawMessage> msg) {
				      while (msg.getStatus() == MessageStatus.NOT_SET) {
					      boolean isCouldAck = false;
					      for (final String url : urls) {
						      BizEvent pushEvent = new BizEvent("Rest.push");
						      Timer.Context timerGlobal = pushTimerGlobal.time();
						      Timer.Context timer = getPushTimer(msg.getTopic()).time();
						      try {
							      pushEvent.addData("topic", sub.getTopic());
							      pushEvent.addData("group", sub.getGroup());
							      pushEvent.addData("refKey", msg.getRefKey());
							      pushEvent.addData("endpoint", url);

							      updateRequestMeter(msg.getTopic());
							      updateRequestSizeHistogram(msg.getTopic(), msg.getBody().getEncodedMessage().length);

							      SubscriptionPushCommand command = new SubscriptionPushCommand(m_httpClient, m_requestConfig,
							            msg, url);
							      HttpResponse pushResponse = command.execute();

							      pushEvent.addData("result", pushResponse.getStatusLine().getStatusCode());
							      if (pushResponse.getStatusLine().getStatusCode() == Response.Status.OK.getStatusCode()) {
								      isCouldAck = true;
								      break;
							      } else if (pushResponse.getStatusLine().getStatusCode() == Response.Status.INTERNAL_SERVER_ERROR
							            .getStatusCode()) {
								      m_logger
								            .warn("Push message failed, will nack, endpoint:{} reason:{} topic:{} partition:{} offset:{} refKey:{}",
								                  url, pushResponse.getStatusLine().getReasonPhrase(), msg.getTopic(),
								                  msg.getPartition(), msg.getOffset(), msg.getRefKey());
								      break;
							      } else {
								      m_logger
								            .warn("Push message failed, will retry, endpoint:{} reason:{} topic:{} partition:{} offset:{} refKey:{}",
								                  url, pushResponse.getStatusLine().getReasonPhrase(), msg.getTopic(),
								                  msg.getPartition(), msg.getOffset(), msg.getRefKey());
							      }

							      updateFailedMeter(msg.getTopic());
							      if (command.isCircuitBreakerOpen()) {
								      long errorCount = command.getMetrics().getHealthCounts().getErrorCount();
								      m_logger.warn("Push message CircuitBreak is open, sleep {} seconds", errorCount);
								      Thread.sleep(1000 * errorCount);
							      }
						      } catch (Exception e) {
							      m_logger.warn("Push message exception", e);
						      } finally {
							      m_bizLogger.log(pushEvent);
							      timer.close();
							      timerGlobal.close();
						      }
					      }

					      if (isCouldAck) {
						      msg.ack();
					      } else {
						      msg.nack();
					      }
				      }
			      }
		      });
		return consumerHolder;
	}

	private void updateFailedMeter(String topic) {
		failedMeterGlobal.mark();

		if (!failedMeterByTopic.containsKey(topic)) {
			Meter failedMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(SubscriptionPushService.class, "SubscriptionPushService", topic, "Failed"));
			failedMeterByTopic.put(topic, failedMeter);
		}

		Meter failedMeter = failedMeterByTopic.get(topic);
		failedMeter.mark();
	}

	private void updateRequestMeter(String topic) {
		requestMeterGlobal.mark();

		if (!requestMeterByTopic.containsKey(topic)) {
			Meter requestMeter = HermesMetricsRegistry.getMetricRegistry().meter(
			      MetricRegistry.name(SubscriptionPushService.class, "SubscriptionPushService", topic, "Request"));
			requestMeterByTopic.put(topic, requestMeter);
		}

		Meter requestMeter = requestMeterByTopic.get(topic);
		requestMeter.mark();
	}

	private void updateRequestSizeHistogram(String topic, int length) {
		requestSizeHistogramGlobal.update(length);

		if (!requestSizeHistogramByTopic.containsKey(topic)) {
			Histogram requestSizeHistogram = HermesMetricsRegistry.getMetricRegistry().histogram(
			      MetricRegistry.name(SubscriptionPushService.class, "SubscriptionPushService", topic, "BodySize"));
			requestSizeHistogramByTopic.put(topic, requestSizeHistogram);
		}

		Histogram requestSizeHistogram = requestSizeHistogramByTopic.get(topic);
		requestSizeHistogram.update(length);
	}
}
