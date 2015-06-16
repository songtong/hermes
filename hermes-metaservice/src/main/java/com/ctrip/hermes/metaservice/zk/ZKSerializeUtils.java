package com.ctrip.hermes.metaservice.zk;

import java.lang.reflect.Type;

import com.alibaba.fastjson.JSON;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ZKSerializeUtils {
	public static byte[] serialize(Object obj) {
		return JSON.toJSONBytes(obj);
	}

	public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
		return JSON.parseObject(bytes, clazz);
	}

	public static <T> T deserialize(byte[] bytes, Type type) {
		return JSON.parseObject(bytes, type);
	}
}
