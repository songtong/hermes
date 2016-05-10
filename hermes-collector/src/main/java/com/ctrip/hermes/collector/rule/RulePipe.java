package com.ctrip.hermes.collector.rule;

import com.ctrip.hermes.collector.pipe.Pipe;
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.state.State;

public class RulePipe extends Pipe<State> {
	private Rule m_rule;
	
	public RulePipe(Rule rule) {
		super(State.class);
		this.m_rule = rule;
	}

	@Override
	public boolean validate(State state) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void doProcess(PipeContext context, State state) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
