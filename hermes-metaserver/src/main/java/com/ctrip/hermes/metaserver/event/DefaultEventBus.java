package com.ctrip.hermes.metaserver.event;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultEventBus implements EventBus {

	private static final Logger log = LoggerFactory.getLogger(DefaultEventBus.class);

	private EventHandlerRegistry m_handlerRegistry = PlexusComponentLocator.lookup(EventHandlerRegistry.class);

	private ExecutorService m_executor = Executors.newSingleThreadExecutor(HermesThreadFactory.create("EventBus", true));

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	@Override
	public void pubEvent(final EventEngineContext context, final Event event) {
		if (m_stopped.get()) {
			return;
		}

		EventType type = event.getType();

		List<EventHandler> handlers = m_handlerRegistry.findHandler(type);

		if (handlers == null || handlers.isEmpty()) {
			log.error("Event handler not found for type {}.", type);
		} else {
			for (final EventHandler handler : handlers) {
				m_executor.submit(new Runnable() {

					@Override
					public void run() {
						try {
							handler.onEvent(context, event);
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
	public void stop() {
		m_stopped.set(true);
		m_executor.shutdownNow();
	}

	@Override
	public boolean isStopped() {
		return m_stopped.get();
	}

}
