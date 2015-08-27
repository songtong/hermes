package com.ctrip.hermes.rest.service;

import java.util.HashSet;
import java.util.List;
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

import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.rest.status.SubscriptionPushStatusMonitor;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

@Named
public class SubscriptionRegisterService {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionRegisterService.class);

	private Set<SubscriptionView> subscriptions = new HashSet<>();

	private Map<SubscriptionView, ConsumerHolder> consumerHolders = new ConcurrentHashMap<>();

	@Inject
	private HttpPushService httpService;

	@Inject
	private SoaPushService soaService;

	@Inject
	private MetaService metaService;

	private ScheduledExecutorService scheduledExecutor;

	public void start() {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("SubscriptionChecker",
		      true));

		SubscriptionPushStatusMonitor.INSTANCE.monitorConsumerHolderSize(consumerHolders);

		scheduledExecutor.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {
					List<SubscriptionView> remoteSubscriptions = metaService.listSubscriptions("RUNNING");
					if (logger.isTraceEnabled()) {
						logger.trace("Received subscriptions: {}", remoteSubscriptions);
					}
					if (remoteSubscriptions == null || remoteSubscriptions.size() == 0) {
						return;
					}

					Set<SubscriptionView> newSubscriptions = new HashSet<>(remoteSubscriptions);
					SetView<SubscriptionView> created = Sets.difference(newSubscriptions, subscriptions);
					SetView<SubscriptionView> removed = Sets.difference(subscriptions, newSubscriptions);

					if (logger.isTraceEnabled()) {
						logger.trace("Current: {}", subscriptions);
						logger.trace("Remote: {}", newSubscriptions);
						logger.trace("ToStart: {}", created);
						logger.trace("ToStop: {}", removed);
					}

					for (SubscriptionView sub : created) {
						boolean isStarted = startSubscription(sub);
						if (isStarted) {
							subscriptions.add(sub);
						}
					}

					if (removed.size() > 0) {
						Set<SubscriptionView> toRemove = new HashSet<>();
						for (SubscriptionView sub : removed) {
							boolean isStopped = stopSubscription(sub);
							if (isStopped) {
								toRemove.add(sub);
							}
						}
						subscriptions.removeAll(toRemove);
					}
				} catch (Exception e) {
					logger.warn("SubscriptionChecker failed", e);
				}
			}

		}, 5, 5, TimeUnit.SECONDS);

		logger.info("SubscriptionRegisterService started");
	}

	public boolean startSubscription(SubscriptionView sub) {
		logger.info("Starting {}", sub);
		ConsumerHolder consumerHolder = null;
		boolean isStarted = true;
		try {
			if ("http".equalsIgnoreCase(sub.getType())) {
				consumerHolder = httpService.startPusher(sub);
			} else if ("soa".equalsIgnoreCase(sub.getType())) {
				consumerHolder = soaService.startPusher(sub);
			} else {
				// FIXME when portal support SOA
				consumerHolder = httpService.startPusher(sub);
			}
		} catch (Exception e) {
			logger.warn("Start {} failed, {}", sub, e);
			isStarted = false;
		}
		if (isStarted) {
			consumerHolders.put(sub, consumerHolder);
			logger.info("Start {} succcessfully", sub);
		}

		return isStarted;
	}

	public void stop() {
		if (scheduledExecutor != null)
			scheduledExecutor.shutdown();
		for (SubscriptionView sub : subscriptions) {
			ConsumerHolder consumerHolder = consumerHolders.remove(sub);
			consumerHolder.close();
		}
		subscriptions.clear();
		logger.info("SubscriptionRegisterService stopped");
	}

	public boolean stopSubscription(SubscriptionView sub) {
		logger.info("Stopping {}", sub);

		ConsumerHolder consumerHolder = consumerHolders.get(sub);
		boolean isClosed = true;
		try {
			consumerHolder.close();
		} catch (Exception e) {
			logger.warn("Stop {} failed, {}", sub, e);
			isClosed = false;
		}
		if (isClosed) {
			consumerHolders.remove(sub);
			logger.info("Stop {} successfully", sub);
		}

		SubscriptionPushStatusMonitor.INSTANCE.removeMonitor(sub.getTopic(), sub.getGroup());

		return isClosed;
	}

}
