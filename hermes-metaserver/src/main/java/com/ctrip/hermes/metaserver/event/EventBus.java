package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.ExecutorService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventBus {

	void pubEvent(Event event);

	ExecutorService getExecutor();

}
