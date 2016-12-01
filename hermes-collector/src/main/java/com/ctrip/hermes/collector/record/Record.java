package com.ctrip.hermes.collector.record;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;
import com.ctrip.hermes.collector.utils.JsonSerializer;


/**
 * @author tenglinxiao
 *
 */
public abstract class Record<T> {
//	public static final String CATEGORY = "category";
//	public static final String TYPE = "type";
//	public static final String DATA = "data";
	
	private String m_id;
	
	private T m_data;
	
	private RecordType m_type;
	
	public Record(RecordType type) {
		this.m_type = type;
		if (this.m_type != null) {
			this.m_id = this.m_type.nextId();
		}
	}

	@JsonIgnore
	public T getData() {
		return m_data;
	}

	public void setData(T data) {
		this.m_data = data;
	}

	@JsonIgnore
	public RecordType getType() {
		return m_type;
	}

	public void setType(RecordType type) {
		this.m_type = type;
	}

	public String getId() {
		return m_id;
	}

	public void setId(String id) {
		m_id = id;
	}
	
	@JsonIgnore
	public abstract long getTimestamp();

//	@SuppressWarnings("unchecked")
//	public void deserialize(JsonNode json) throws DeserializeException {
//		// Deserialize for this object.
//		JsonSerializer.getInstance().deserialize(json, this);
//		m_type = RecordType.findByCategoryType(json.get(CATEGORY).asText(), json.get(TYPE).asText());
//		m_data = (T)JsonSerializer.getInstance().deserialize(json, m_data.getClass());
//	}
//
//	@Override
//	public JsonNode serialize() throws SerializeException {
//		if (m_type == null) {
//			throw new SerializeException("Member m_type is required on serialization!");
//		}
//		ObjectNode json = (ObjectNode)JsonSerializer.getInstance().serialize(this, false);
//		json.put(DATA, m_data.serialize());
//		json.put(CATEGORY, m_type.getCategory().name());
//		json.put(TYPE, m_type.getName());
//		return json;
//	}
	
	public String toString() {
		try {
			return JsonSerializer.getInstance().serialize(this, false).toString();
		} catch (SerializeException e) {
			return null;
		}
	}
}
