package com.ctrip.hermes.metaserver.event;

import java.util.List;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventHandlerRegistry {

	List<EventHandler> findHandler(EventType type);

}
