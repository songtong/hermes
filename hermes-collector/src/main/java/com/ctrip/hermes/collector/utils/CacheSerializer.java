package com.ctrip.hermes.collector.utils;

import java.nio.ByteBuffer;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import com.ctrip.hermes.collector.state.State;

public class CacheSerializer implements Serializer<State> {
	private static ObjectMapper mapper = new ObjectMapper();
	public CacheSerializer(ClassLoader classLoader, FileBasedPersistenceContext persistenceContext) {
	}


	@Override
	public ByteBuffer serialize(State object) throws SerializerException {
		try {
			byte[] bytes = JsonSerializer.getInstance().serialize(object, true).toString().getBytes("UTF-8");
			return ByteBuffer.wrap(bytes);
		} catch (Exception e) {
			throw new SerializerException(e);
		}
	}

	@Override
	public State read(ByteBuffer binary) throws ClassNotFoundException,
			SerializerException {
		try {
			JsonNode json = mapper.readTree(binary.array());
			return (State)JsonSerializer.getInstance().deserialize(json);
		} catch(Exception e) {
			throw new SerializerException(e);
		}
	}

	@Override
	public boolean equals(State object, ByteBuffer binary)
			throws ClassNotFoundException, SerializerException {
		return binary.compareTo(serialize(object)) == 0;
	}

}
