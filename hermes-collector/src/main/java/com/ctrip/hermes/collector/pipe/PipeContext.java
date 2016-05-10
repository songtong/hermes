package com.ctrip.hermes.collector.pipe;

import com.ctrip.hermes.collector.pipe.Processor.ProcessContext;
import com.ctrip.hermes.collector.state.State;


public class PipeContext implements ProcessContext {
	private boolean m_resetEveryPipe;
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
	
	public boolean isResetEveryPipe() {
		return m_resetEveryPipe;
	}

	public void setResetEveryPipe(boolean resetEveryPipe) {
		m_resetEveryPipe = resetEveryPipe;
	}

	public void reset() {
		this.m_isAbort = false;
	}
}
