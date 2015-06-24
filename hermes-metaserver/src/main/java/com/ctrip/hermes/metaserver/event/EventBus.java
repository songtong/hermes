package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventBus {

	void pubEvent(EventEngineContext context, Event event);

	void stop();

	boolean isStopped();

}
