package com.ctrip.hermes.rest.service;

import java.util.List;
import java.util.Map;

public interface SubscribeRegistry {

	public List<Subscription> getSubscriptions(String topicName);

	public Map<String, List<Subscription>> getSubscriptions();

	public void register(Subscription subscription);

	public void unregister(Subscription subscription);
}
