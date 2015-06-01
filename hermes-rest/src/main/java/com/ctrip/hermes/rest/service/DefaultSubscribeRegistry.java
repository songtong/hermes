package com.ctrip.hermes.rest.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;

@Named(type = SubscribeRegistry.class)
public class DefaultSubscribeRegistry implements SubscribeRegistry {

	private Map<String, List<Subscription>> registries = new HashMap<>();

	private Map<Subscription, ConsumerHolder> consumerHolders = new HashMap<>();

	@Inject
	private MessagePushService pushService;

	@Override
	public Map<String, List<Subscription>> getSubscriptions() {
		return Collections.unmodifiableMap(registries);
	}

	@Override
	public List<Subscription> getSubscriptions(String topicName) {
		return registries.get(topicName);
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

}
