package com.ctrip.hermes.collector.hub;

import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.State;

public class RulesEngineTest extends RulesEngine {
	//private List<RuleEvent> m_events = new ArrayList<RuleEvent>();

	@Override
	public void doProcess(PipeContext context, State state)
			throws Exception {
		//m_events.add(event);
	}

}
