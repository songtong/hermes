package com.ctrip.hermes.collector.pipe;

import com.ctrip.hermes.collector.state.State;


public class PipeContext {
	private boolean m_isAbort;
	private State state;
	
	public void abort() {
		this.m_isAbort = true;
	}
	
	public boolean isAbort() {
		return this.m_isAbort;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
}
