package com.ctrip.hermes.rest.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Meter;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

@Named(type = SubscribeRegistry.class)
public class DefaultSubscribeRegistry implements SubscribeRegistry, Initializable {

	private static final Logger logger = LoggerFactory.getLogger(DefaultSubscribeRegistry.class);

	private Map<String, List<Subscription>> registries = new ConcurrentHashMap<>();

	private Map<Subscription, ConsumerHolder> consumerHolders = new ConcurrentHashMap<>();

	@Inject
	private MessagePushService pushService;

	@Inject
	private MetricsManager m_metricsManager;

	private ScheduledExecutorService scheduledExecutor;

	@Override
	public Map<String, List<Subscription>> getSubscriptions() {
		return Collections.unmodifiableMap(registries);
	}

	@Override
	public List<Subscription> getSubscriptions(String topicName) {
		return Collections.unmodifiableList(registries.get(topicName));
	}

	@Override
	public synchronized void register(Subscription subscription) {
		if (!registries.containsKey(subscription.getTopic())) {
			registries.put(subscription.getTopic(), new ArrayList<Subscription>());
		}

		List<Subscription> subscriptions = registries.get(subscription.getTopic());
		subscriptions.add(subscription);

		ConsumerHolder consumerHolder = pushService.startPusher(subscription);
		consumerHolders.put(subscription, consumerHolder);
	}

	@Override
	public synchronized void unregister(Subscription subscription) {
		if (registries.containsKey(subscription.getTopic())) {
			List<Subscription> subscriptions = registries.get(subscription.getTopic());
			subscriptions.remove(subscription);

			ConsumerHolder consumerHolder = consumerHolders.remove(subscription);
			consumerHolder.close();
		}
	}

	@Override
	public void initialize() throws InitializationException {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("SubscriptionChecker",
		      true));

		scheduledExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<Subscription, ConsumerHolder> entry : consumerHolders.entrySet()) {
					Subscription sub = entry.getKey();
					Meter failed_meter = m_metricsManager.meter("push_fail", sub.getTopic(), sub.getGroupId(), sub
					      .getEndpoints().toString());
					if (failed_meter.getOneMinuteRate() > 0.5) {
						logger.warn("Too many failed in the past minute {}, unregister it", failed_meter.getOneMinuteRate());
						unregister(sub);
					}
				}
			}

		}, 5, 5, TimeUnit.SECONDS);
	}

}
