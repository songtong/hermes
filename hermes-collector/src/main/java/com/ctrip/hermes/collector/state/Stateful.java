package com.ctrip.hermes.collector.state;


public interface Stateful {
	// Determine whether it's expired.
	public boolean expired();
	// Get state generated timestamp.
	public long getTimestamp();
	// Update the stateful object.
	public void update(Stateful s);
}
