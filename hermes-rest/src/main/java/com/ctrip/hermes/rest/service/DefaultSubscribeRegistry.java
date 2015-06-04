package com.ctrip.hermes.rest.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.codahale.metrics.Meter;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Subscription;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

@Named(type = SubscribeRegistry.class)
public class DefaultSubscribeRegistry implements SubscribeRegistry {

	private static final Logger logger = LoggerFactory.getLogger(DefaultSubscribeRegistry.class);

	private Set<Subscription> subscriptions = new HashSet<>();

	private Map<Subscription, ConsumerHolder> consumerHolders = new ConcurrentHashMap<>();

	@Inject
	private MessagePushService pushService;

	@Inject
	private MetricsManager m_metricsManager;

	@Inject
	private MetaService m_metaService;

	private ScheduledExecutorService scheduledExecutor;

	@Override
	public synchronized void register(Subscription subscription) {
		if (!subscriptions.contains(subscription)) {
			subscriptions.add(subscription);
		}

		ConsumerHolder consumerHolder = pushService.startPusher(subscription);
		consumerHolders.put(subscription, consumerHolder);
	}

	@Override
	public synchronized void unregister(Subscription subscription) {
		if (subscriptions.contains(subscription)) {
			subscriptions.remove(subscription);

			ConsumerHolder consumerHolder = consumerHolders.remove(subscription);
			consumerHolder.close();
		}
	}

	@Override
	public void start() {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("SubscriptionChecker",
		      true));

		scheduledExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<Subscription, ConsumerHolder> entry : consumerHolders.entrySet()) {
					Subscription sub = entry.getKey();
					Meter failed_meter = m_metricsManager.meter("push_fail", sub.getTopic(), sub.getGroup(), sub
					      .getEndpoints().toString());
					if (failed_meter.getOneMinuteRate() > 0.5) {
						logger.warn("Too many failed in the past minute {}, unregister it", failed_meter.getOneMinuteRate());
						unregister(sub);
					}
				}
			}

		}, 5, 5, TimeUnit.SECONDS);

		scheduledExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				Set<Subscription> newSubscriptions = new HashSet<>(m_metaService.listSubscriptions());
				SetView<Subscription> created = Sets.difference(newSubscriptions, subscriptions);
				SetView<Subscription> removed = Sets.difference(subscriptions, newSubscriptions);
				for (Subscription sub : created) {
					logger.info("register: " + sub);
					register(sub);
				}
				for (Subscription sub : removed) {
					logger.info("unregister: " + sub);
					unregister(sub);
				}
			}

		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		scheduledExecutor.shutdown();
		for (Subscription sub : subscriptions) {
			unregister(sub);
		}
	}

}
