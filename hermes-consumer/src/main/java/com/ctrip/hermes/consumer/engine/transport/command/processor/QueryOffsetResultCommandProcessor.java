package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.QueryOffsetResultCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class QueryOffsetResultCommandProcessor implements CommandProcessor {

	@Inject
	private QueryOffsetResultMonitor m_queryOffsetResultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_QUERY_OFFSET);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryOffsetResultCommand cmd = (QueryOffsetResultCommand) ctx.getCommand();
		m_queryOffsetResultMonitor.resultReceived(cmd);
	}

}
