package com.ctrip.hermes.metaserver.event;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EventBus.class)
public class DefaultEventBus implements EventBus, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultEventBus.class);

	@Inject
	private EventHandlerRegistry m_handlerRegistry;

	@Inject
	private Guard m_guard;

	private ExecutorService m_executor;

	@Override
	public void pubEvent(final Event event) {

		if (event.getVersion() != m_guard.getVersion()) {
			return;
		}

		EventType type = event.getType();

		List<EventHandler> handlers = m_handlerRegistry.findHandler(type);

		if (handlers == null || handlers.isEmpty()) {
			log.error("Event handler not found for type {}.", type);
		} else {
			event.setEventBus(this);

			for (final EventHandler handler : handlers) {
				m_executor.submit(new Runnable() {

					@Override
					public void run() {
						try {
							handler.onEvent(event);
						} catch (Exception e) {
							log.error("Exception occurred while processing event {} in handler {}", event.getType(),
							      handler.getName(), e);
						}
					}
				});
			}
		}

	}

	@Override
	public void initialize() throws InitializationException {
		m_executor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("EventBus", true));
	}

	@Override
	public ExecutorService getExecutor() {
		return m_executor;
	}

}
