package com.ctrip.hermes.rest.service.json;

import java.lang.reflect.Type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONLexer;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;

public class CharSequenceDeserializer implements ObjectDeserializer {
	public final static CharSequenceDeserializer instance = new CharSequenceDeserializer();

	@SuppressWarnings("unchecked")
	public <T> T deserialze(DefaultJSONParser parser, Type clazz, Object fieldName) {
		return (T) deserialze(parser);
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialze(DefaultJSONParser parser) {
		final JSONLexer lexer = parser.getLexer();
		if (lexer.token() == JSONToken.LITERAL_STRING) {
			String val = lexer.stringVal();
			lexer.nextToken(JSONToken.COMMA);
			return (T) val;
		}

		if (lexer.token() == JSONToken.LITERAL_INT) {
			Number val = lexer.integerValue();
			lexer.nextToken(JSONToken.COMMA);
			return (T) val.toString();
		}

		Object value = parser.parse();

		if (value == null) {
			return null;
		}

		return (T) JSON.toJSONString(value);
	}

	public int getFastMatchToken() {
		return JSONToken.LITERAL_STRING;
	}
}
