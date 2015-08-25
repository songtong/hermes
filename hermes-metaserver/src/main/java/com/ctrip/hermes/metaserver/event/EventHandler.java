package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventHandler {

	public void onEvent(Event event) throws Exception;

	public String getName();

	public EventType eventType();

}
