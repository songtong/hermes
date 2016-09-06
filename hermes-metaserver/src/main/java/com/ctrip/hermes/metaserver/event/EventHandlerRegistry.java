package com.ctrip.hermes.metaserver.event;

import java.util.List;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventHandlerRegistry {

	List<EventHandler> findHandler(EventType type);

	public void forEachHandler(Function fun);

	public interface Function {
		public void apply(EventHandler handler);
	}

}
