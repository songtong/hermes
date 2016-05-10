package com.ctrip.hermes.collector.state;

import java.util.Observable;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.record.Serializable;
import com.ctrip.hermes.collector.utils.JsonSerializer;

public abstract class State extends Observable implements Stateful,
		Serializable<JsonNode> {

	// Es relatives.
	private String m_index;
	private String m_type = "logs";
	private long m_timestamp;
	private Object m_id;

	//
	private State m_nextState;
	private State m_prevState;
	private long m_expiredTime;
	private boolean m_sync;
	
	public State() {
	}

	public State(Object id) {
		this.m_id = id;
	}

	@JsonIgnore
	public Object getId() {
		if (this.m_id == null) {
			return generateId();
		}
		return m_id;
	}

	public void setId(Object id) {
		m_id = id;
	}

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
	public long getTimestamp() {
		return m_timestamp;
	}

	public void setTimestamp(long timestamp) {
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

	public boolean expired() {
		return m_expiredTime > 0 && m_expiredTime < System.currentTimeMillis();
	}

	@JsonIgnore
	public long getExpiredTime() {
		return m_expiredTime;
	}
	
	@JsonIgnore
	public boolean isSync() {
		return m_sync;
	}

	public void setSync(boolean sync) {
		m_sync = sync;
	}

	public void setExpiredTime(long expiredTime) {
		m_expiredTime = expiredTime;
	}

	public void update(final Stateful s) {
		if (s == null) {
			return;
		}

		State state = (State) s;

		// Update the state based on the current one if it's a new state.
		if (!this.equals(state)) {
			doUpdate(state);

			// Update next state if has.
			if (m_nextState != null) {
				m_nextState.update(state);
			}
			
			notifyObservers();
		}
	}
	
	public void notifyObservers() {
		setChanged();
		super.notifyObservers(this);
	}

	protected abstract void doUpdate(State state);
	
	protected abstract Object generateId();

	@Override
	public JsonNode serialize() throws SerializeException {
		return JsonSerializer.getInstance().serialize(this, true);
	}

	@Override
	public void deserialize(JsonNode json) throws DeserializeException {
		JsonSerializer.getInstance().deserialize(json, this);
	}

	public boolean equals(Object obj) {
		return obj.getClass() == this.getClass()
				&& ((State) obj).getId() == this.getId();
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
