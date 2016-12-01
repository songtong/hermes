package com.ctrip.hermes.collector.rule;

/**
 * @author tenglinxiao
 *
 */
public interface Event {
	
	public String getEventType();
	
	public Object getData();
}
