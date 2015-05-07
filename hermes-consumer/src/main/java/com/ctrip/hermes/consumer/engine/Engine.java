package com.ctrip.hermes.consumer.engine;

import java.util.List;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public abstract class Engine {

	public abstract SubscribeHandle start(List<Subscriber> subscribers);

	public static Engine getInstance() {
		return PlexusComponentLocator.lookup(Engine.class);
	}
}
