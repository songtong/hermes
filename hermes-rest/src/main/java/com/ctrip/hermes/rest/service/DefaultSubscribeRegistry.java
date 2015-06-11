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
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

@Named(type = SubscribeRegistry.class)
public class DefaultSubscribeRegistry implements SubscribeRegistry {

	private static final Logger logger = LoggerFactory.getLogger(DefaultSubscribeRegistry.class);

	private Set<SubscriptionView> subscriptions = new HashSet<>();

	private Map<SubscriptionView, ConsumerHolder> consumerHolders = new ConcurrentHashMap<>();

	@Inject
	private MessagePushService pushService;

	@Inject
	private MetricsManager m_metricsManager;

	@Inject
	private MetaService m_metaService;

	private ScheduledExecutorService scheduledExecutor;

	@Override
	public void start() {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("SubscriptionChecker",
		      true));

		scheduledExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				for (Map.Entry<SubscriptionView, ConsumerHolder> entry : consumerHolders.entrySet()) {
					SubscriptionView sub = entry.getKey();
					Meter failed_meter = m_metricsManager.meter("push_fail", sub.getTopic(), sub.getGroup(), sub
					      .getEndpoints().toString());
					if (failed_meter.getOneMinuteRate() > 0.5) {
						logger.warn("Too many failed in the past minute {}, suspend {}", failed_meter.getOneMinuteRate(),
						      sub.getId());
						ConsumerHolder consumerHolder = consumerHolders.remove(sub);
						consumerHolder.close();
					}
				}
			}

		}, 5, 5, TimeUnit.SECONDS);

		scheduledExecutor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				Set<SubscriptionView> newSubscriptions = new HashSet<>(m_metaService.listSubscriptions());
				SetView<SubscriptionView> created = Sets.difference(newSubscriptions, subscriptions);
				SetView<SubscriptionView> removed = Sets.difference(subscriptions, newSubscriptions);
				for (SubscriptionView sub : created) {
					logger.info("register: " + sub);

					ConsumerHolder consumerHolder = pushService.startPusher(sub);
					consumerHolders.put(sub, consumerHolder);
				}
				subscriptions.addAll(created);
				for (SubscriptionView sub : removed) {
					logger.info("unregister: " + sub);

					ConsumerHolder consumerHolder = consumerHolders.remove(sub);
					consumerHolder.close();
				}
				subscriptions.removeAll(removed);
			}

		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		scheduledExecutor.shutdown();
		for (SubscriptionView sub : subscriptions) {
			ConsumerHolder consumerHolder = consumerHolders.remove(sub);
			consumerHolder.close();
		}
	}

}
