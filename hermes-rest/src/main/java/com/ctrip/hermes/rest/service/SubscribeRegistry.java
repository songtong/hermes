package com.ctrip.hermes.rest.service;

import com.ctrip.hermes.meta.entity.Subscription;

public interface SubscribeRegistry {

	public void register(Subscription subscription);

	public void unregister(Subscription subscription);
	
	public void start();
	
	public void stop();
}
