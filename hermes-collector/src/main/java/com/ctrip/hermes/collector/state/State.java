package com.ctrip.hermes.collector.state;

import java.util.Observable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.record.Serializable;
import com.ctrip.hermes.collector.utils.JsonSerializer;

public abstract class State extends Observable implements Serializable<JsonNode> {
	private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
	
	// Es relatives.
	private String m_index;
	private String m_type = "data";
	private Long m_timestamp;
	private Object m_id;
	
	// 
	private TimeUnit m_timeUnit;
	private int m_interval;
	private int m_retainedInterval;
	private State m_nextState;
	private State m_prevState;
	private State m_lastState;
	private int m_size;
	public State() {
		this(ID_GENERATOR.incrementAndGet());
	}
	public State(Object id) {
		this.m_id = id;
	}
	@JsonIgnore
	public Object getId() {
		return m_id;
	}
	public void setId(Object id) {
		m_id = id;
	}
	@JsonIgnore
	public String getIndex() {
		return m_index;
	}
	public void setIndex(String index) {
		m_index = index;
	}
	@JsonIgnore
	public String getType() {
		return m_type;
	}
	public void setType(String type) {
		m_type = type;
	}
	@JsonProperty("@timestamp")
	public Long getTimestamp() {
		return m_timestamp;
	}
	public void setTimestamp(Long timestamp) {
		m_timestamp = timestamp;
	}
	@JsonIgnore
	public State getNextState() {
		return m_nextState;
	}
	public void setNextState(State nextState) {
		m_nextState = nextState;
	}
	@JsonIgnore
	public State getPrevState() {
		return m_prevState;
	}
	public void setPrevState(State prevState) {
		m_prevState = prevState;
	}
	@JsonIgnore
	public TimeUnit getTimeUnit() {
		return m_timeUnit;
	}
	public void setTimeUnit(TimeUnit timeUnit) {
		m_timeUnit = timeUnit;
	}
	@JsonIgnore
	public int getInterval() {
		return m_interval;
	}
	public void setInterval(int interval) {
		m_interval = interval;
	}
	@JsonIgnore
	public int getRetainedInterval() {
		return m_retainedInterval;
	}
	public void setRetainedInterval(int retainedInterval) {
		m_retainedInterval = retainedInterval;
	}
	protected void addState(State state) {
		// Add new state to the bi-directional list.
		if (this.m_nextState == null) {
			this.m_nextState = state;
			this.m_lastState = state;
		} else {
			this.m_lastState.setNextState(state);
			state.setPrevState(this.m_lastState);
			this.m_lastState = state;
		}
		
		// If Already up to the retained size, remove the last state.
		if (m_size == m_retainedInterval) {
			State removed = m_lastState;
			m_lastState = m_lastState.getPrevState();
			m_lastState.setNextState(null);
			removed.setPrevState(null);
		}
	}
	public int size() {
		return m_size;
	}
	protected boolean expired(State state) {
		return state != null && state.getTimestamp() <= m_timestamp;
	}
	public void update(final State state) {
		if (state == null) {
			return;
		}

		// If it's a new state, 
		if (!this.equals(state)) {
			addState(state);
			// Update state based on their old states.
			if (m_nextState != null) {
				m_nextState.update(state);
			}
		}
		
		notifyObservers(this);
	}
	protected abstract void doUpdate(State state);
	@Override
	public JsonNode serialize() throws SerializeException {
		return JsonSerializer.getInstance().serialize(this, true);
	}
	@Override
	public void deserialize(JsonNode json) throws DeserializeException {
		JsonSerializer.getInstance().deserialize(json, this);
	}
	
	public boolean equals(Object obj) {
		return obj.getClass() == this.getClass() && ((State)obj).getId() == this.getId();
	}
	
	public String toString() {
		try {
			return this.serialize().toString();
		} catch (SerializeException e) {
			e.printStackTrace();
			return null;
		}
	}
	
}
