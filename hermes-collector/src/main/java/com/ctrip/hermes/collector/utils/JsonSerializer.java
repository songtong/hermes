package com.ctrip.hermes.collector.utils;

import java.util.Collection;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.node.ObjectNode;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;

/**
 * @author tenglinxiao
 *
 */
public class JsonSerializer {
	private static final String TYPE = "__serialize_type__";
	private static JsonSerializer m_serializer = null;
	private ObjectMapper m_mapper = new ObjectMapper();
	
	private JsonSerializer() {
		m_mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		m_mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
	}
	
	public ObjectMapper getMapper() {
		return m_mapper;
	}
	
	public Object deserialize(JsonNode json) throws DeserializeException {
		String type = json.get(TYPE).asText();
		if (type == null) {
			throw new DeserializeException("Type is not offered in deserialization object!");
		}
		
		try {
			return deserialize(json, Class.forName(type));
		} catch (ClassNotFoundException e) {
			throw new DeserializeException(e);
		}
	}

	public Object deserialize(JsonNode json, Class<?> clz) throws DeserializeException {
		try {
			 return getMapper().treeToValue(json, clz);
		} catch (Exception e) {
			throw new DeserializeException(e);
		}
	}
	
	public Object deserialize(JsonNode json, Object obj) throws DeserializeException {
		try {
			 return getMapper().readerForUpdating(obj).readValue(json);
		} catch (Exception e) {
			throw new DeserializeException(e);
		}
	}

	public JsonNode serialize(Object obj, boolean includeType) throws SerializeException {
		try {
			JsonNode data = getMapper().valueToTree(obj);
			if (includeType) {
				if (obj instanceof Collection) {
					throw new SerializeException("Serialized object can NOT be instance of a class extending Collection!");
				}
				((ObjectNode)data).put(TYPE, obj.getClass().getName());
				return data;
			}
			return data;
		} catch (Exception e) {
			throw new SerializeException(e);
		}
	}
	
	public static JsonSerializer getInstance() {
		if (m_serializer == null) {
			synchronized (JsonSerializer.class) {
				if (m_serializer == null) {
					m_serializer = new JsonSerializer();
				}
			}
		}
		return m_serializer;
	}
}
