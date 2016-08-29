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
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.EventHandlerRegistry.Function;

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
		if (!m_guard.pass(event.getVersion())) {
			log.info("Can't pub event, due to event expired(event:{}, version:{}).", event.getType(), event.getVersion());
			return;
		}

		final EventType type = event.getType();

		List<EventHandler> handlers = m_handlerRegistry.findHandler(type);

		if (handlers == null || handlers.isEmpty()) {
			log.error("Event handler not found for type {}.", type);
		} else {
			for (final EventHandler handler : handlers) {
				m_executor.submit(new Runnable() {

					@Override
					public void run() {
						if (!m_guard.pass(event.getVersion())) {
							log.info("Event expired(event:{}, version:{}).", event.getType(), event.getVersion());
							return;
						}

						long start = System.currentTimeMillis();
						try {
							handler.onEvent(event);
						} catch (Exception e) {
							log.error("Exception occurred while processing event {} in handler {}", event.getType(),
							      handler.getName(), e);
						} finally {
							log.info("Handle event.(type={}, duration={}ms).", type, (System.currentTimeMillis() - start));
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

	@Override
	public void submit(final long version, final Task task) {
		if (m_guard.pass(version)) {
			m_executor.submit(new Runnable() {

				@Override
				public void run() {
					if (m_guard.pass(version)) {
						task.run();
					} else {
						log.debug("Can't run task, due to event expired(version:{}).", version);
						task.onGuardNotPass();
					}
				}
			});
		} else {
			log.debug("Can't submit task, due to event expired(version:{}).", version);
			task.onGuardNotPass();
		}
	}

	@Override
	public void start(final ClusterStateHolder clusterStateHolder) {
		m_handlerRegistry.forEachHandler(new Function() {

			@Override
			public void apply(EventHandler handler) {
				handler.setClusterStateHolder(clusterStateHolder);
				handler.setEventBus(DefaultEventBus.this);
			}
		});
	}

}
