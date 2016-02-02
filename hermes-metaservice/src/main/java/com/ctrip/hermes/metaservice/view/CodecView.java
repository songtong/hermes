package com.ctrip.hermes.metaservice.view;

import java.util.List;
import java.util.Objects;

import com.ctrip.hermes.meta.entity.Property;

public class CodecView {

	private String type;

	private List<Property> properties;

	public CodecView() {

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

	public int hashCode() {
		return Objects.hash(this.type, this.properties);
	}
}
