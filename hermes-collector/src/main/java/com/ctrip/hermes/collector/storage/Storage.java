package com.ctrip.hermes.collector.storage;

import com.ctrip.hermes.collector.state.State;

public abstract class Storage {
	public abstract boolean save(State state);
}
