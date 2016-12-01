package com.ctrip.hermes.collector.record;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.JsonSerializer;

/**
 * @author tenglinxiao
 *
 */
public abstract class RecordContent implements Serializable<JsonNode>, Bindable<JsonNode> {
	public RecordContent() {}

	@Override
	public JsonNode serialize() throws SerializeException {
		return JsonSerializer.getInstance().serialize(this, false);
	}

	@Override
	public void deserialize(JsonNode json) throws DeserializeException {
		JsonSerializer.getInstance().deserialize(json, this);
	}

	public String toString() {
		try {
			return serialize().toString();
		} catch (SerializeException e) {
			return null;
		}
	}
}
