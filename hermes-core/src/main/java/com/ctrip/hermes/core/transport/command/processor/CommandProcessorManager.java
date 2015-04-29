package com.ctrip.hermes.core.transport.command.processor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;

@Named(type = CommandProcessorManager.class)
public class CommandProcessorManager implements Initializable, LogEnabled {

	@Inject
	private CommandProcessorRegistry m_registry;

	private Map<CommandProcessor, ExecutorService> m_executors = new ConcurrentHashMap<>();

	// TODO
	private Logger m_logger;

	public void offer(final CommandProcessorContext ctx) {
		Command cmd = ctx.getCommand();
		CommandType type = cmd.getHeader().getType();
		final CommandProcessor processor = m_registry.findProcessor(type);
		if (processor == null) {
			m_logger.error(String.format("Command processor not found for type %s", type));
		} else {
			ExecutorService executorService = m_executors.get(processor);

			if (executorService == null) {
				throw new IllegalArgumentException(String.format("No executor associated to processor %s", processor
				      .getClass().getSimpleName()));
			} else {
				executorService.submit(new Runnable() {

					@Override
					public void run() {
						try {
							processor.process(ctx);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		}
	}

	@Override
	public void initialize() throws InitializationException {
		Set<CommandProcessor> cmdProcessors = m_registry.listAllProcessors();

		for (CommandProcessor cmdProcessor : cmdProcessors) {

			String threadNamePrefix = String.format("CmdProcessor-%s", cmdProcessor.getClass().getSimpleName());
			if (cmdProcessor.getClass().isAnnotationPresent(SingleThreaded.class)) {
				// TODO config ThreadFactory
				// TODO when do we need to shutdown executorService;
				m_executors.put(cmdProcessor, Threads.forPool().getFixedThreadPool(threadNamePrefix, 1));
			} else {
				// TODO config thread pool
				m_executors.put(cmdProcessor, Threads.forPool().getFixedThreadPool(threadNamePrefix, 10));
			}
		}

	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}
}
