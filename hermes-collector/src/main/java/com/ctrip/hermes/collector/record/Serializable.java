package com.ctrip.hermes.collector.record;

import com.ctrip.hermes.collector.exception.SerializationException.DeserializeException;
import com.ctrip.hermes.collector.exception.SerializationException.SerializeException;

/**
 * @author tenglinxiao
 *
 * */
public interface Serializable<T> {
	//
	public T serialize() throws SerializeException;
	// 
	public void deserialize(T t) throws DeserializeException ;
}
