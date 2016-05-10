package com.ctrip.hermes.collector.state;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tenglinxiao
 *
 */
public class States extends State {
	private Map<Object, State> m_states = new HashMap<Object, State>();
	
	public States() {
		super();
	}

	public Map<Object, State> getStates() {
		return m_states;
	}

	public void setStates(Map<Object, State> states) {
		m_states = states;
	}
	
	public void update(State state) {
		this.m_states.put(state.getId(), state);
	}
	
	public int size() {
		return m_states.size();
	}

	@Override
	protected void doUpdate(State state) {
		// DO NOTHING!
	}
}
