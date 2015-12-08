package com.ctrip.hermes.consumer.engine.transport.command.processor;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.consumer.engine.monitor.QueryOffsetResultMonitor;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v3.QueryOffsetResultCommandV3;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class QueryOffsetResultCommandProcessor implements CommandProcessor {

	@Inject
	private QueryOffsetResultMonitor m_queryOffsetResultMonitor;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.RESULT_QUERY_OFFSET_V3);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		QueryOffsetResultCommandV3 cmd = (QueryOffsetResultCommandV3) ctx.getCommand();
		m_queryOffsetResultMonitor.resultReceived(cmd);
	}

}