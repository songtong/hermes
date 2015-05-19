package com.ctrip.hermes.meta.pojo;

import java.util.List;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Property;

public class CodecView {
	private String type;

	private List<Property> properties;

	public CodecView() {

	}

	public CodecView(Codec codec) {
		this.type = codec.getType();
	}

	public List<Property> getProperties() {
		return properties;
	}

	public String getType() {
		return type;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Codec toMetaCodec() {
		Codec codec = new Codec();
		codec.setType(this.type);
		return codec;
	}

}
