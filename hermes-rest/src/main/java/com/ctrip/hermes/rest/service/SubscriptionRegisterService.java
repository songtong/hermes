package com.ctrip.hermes.rest.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import com.ctrip.hermes.admin.core.service.SubscriptionService;
import com.ctrip.hermes.admin.core.service.SubscriptionService.SubscriptionStatus;
import com.ctrip.hermes.admin.core.view.SubscriptionView;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.rest.status.SubscriptionPushStatusMonitor;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

@Named
public class SubscriptionRegisterService {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionRegisterService.class);

	private Set<SubscriptionView> subscriptions = new HashSet<>();

	private Map<SubscriptionView, ConsumerHolder> consumerHolders = new ConcurrentHashMap<>();

	private Map<SubscriptionView, ListenerHolder> listenerHolders = new ConcurrentHashMap<>();
	
	@Inject
	private HttpPushService httpService;

	@Inject
	private SoaPushService soaService;

	@Inject
	private SubscriptionService subscriptionService;

	private ScheduledExecutorService scheduledExecutor;

	public void start() {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("SubscriptionChecker",
		      true));

		SubscriptionPushStatusMonitor.INSTANCE.monitorConsumerHolderSize(consumerHolders);

		scheduledExecutor.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {
					List<SubscriptionView> remoteSubscriptions = subscriptionService.getSubscriptions(SubscriptionStatus.RUNNING);
					if (logger.isTraceEnabled()) {
						logger.trace("Received subscriptions: {}", remoteSubscriptions);
					}
					if (remoteSubscriptions == null) {
						return;
					}

					Set<SubscriptionView> newSubscriptions = new HashSet<>(remoteSubscriptions);
					SetView<SubscriptionView> created = Sets.difference(newSubscriptions, subscriptions);
					SetView<SubscriptionView> removed = Sets.difference(subscriptions, newSubscriptions);

					if (created.size() > 0 || removed.size() > 0) {
						logger.info("Current: {}", subscriptions);
						logger.info("Remote: {}", newSubscriptions);
						logger.info("ToStart: {}", created);
						logger.info("ToStop: {}", removed);
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
			if (sub.getType() != null && "qmq".equalsIgnoreCase(sub.getType())) {
				ListenerHolder holder = httpService.startQmqPusher(sub);
				listenerHolders.put(sub, holder);
			} else {
				consumerHolder = httpService.startPusher(sub);
				consumerHolders.put(sub, consumerHolder);
			}
			logger.info("Start {} succcessfully", sub);
		} catch (Exception e) {
			logger.warn("Start {} failed, {}", sub, e);
			isStarted = false;
		}

		return isStarted;
	}

	public void stop() {
		if (scheduledExecutor != null)
			scheduledExecutor.shutdown();
		for (SubscriptionView sub : subscriptions) {
			if (sub.getType() != null && sub.getType().equalsIgnoreCase("qmq")) {
				ListenerHolder holder = listenerHolders.get(sub);
				holder.stopListen();
			} else {
				ConsumerHolder consumerHolder = consumerHolders.remove(sub);
				consumerHolder.close();
			}
		}
		subscriptions.clear();
		logger.info("SubscriptionRegisterService stopped");
	}

	public boolean stopSubscription(SubscriptionView sub) {
		logger.info("Stopping {}", sub);

		boolean isClosed = true;
		try {
			if (sub.getType() != null && "qmq".equalsIgnoreCase(sub.getType())) {
				ListenerHolder holder = listenerHolders.get(sub);
				holder.stopListen();
				listenerHolders.remove(sub);
			} else {
				ConsumerHolder consumerHolder = consumerHolders.get(sub);
				consumerHolder.close();
				consumerHolders.remove(sub);
			}
			logger.info("Stop {} successfully", sub);
		} catch (Exception e) {
			logger.warn("Stop {} failed, {}", sub, e);
			isClosed = false;
		}

		SubscriptionPushStatusMonitor.INSTANCE.removeMonitor(sub.getTopic(), sub.getGroup());

		return isClosed;
	}

	public Set<SubscriptionView> listSubscriptions() {
		return this.subscriptions;
	}

}
