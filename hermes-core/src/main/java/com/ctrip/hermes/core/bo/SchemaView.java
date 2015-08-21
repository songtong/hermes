package com.ctrip.hermes.core.bo;

import java.util.Date;
import java.util.List;

import com.ctrip.hermes.meta.entity.Property;

public class SchemaView {
	private Long id;

	private String name;

	private String type;

	private Integer version;

	private Date createTime;

	private String compatibility;

	private String description;

	private List<Property> properties;

	private String schemaPreview;

	private Long topicId;
	
	private int avroId;

	public SchemaView() {

	}

	public String getCompatibility() {
		return compatibility;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public String getDescription() {
		return description;
	}

	public Long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public String getSchemaPreview() {
		return schemaPreview;
	}

	public Long getTopicId() {
		return topicId;
	}

	public String getType() {
		return type;
	}

	public Integer getVersion() {
		return version;
	}

	public void setCompatibility(String compatibility) {
		this.compatibility = compatibility;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public void setSchemaPreview(String schemaPreview) {
		this.schemaPreview = schemaPreview;
	}

	public void setTopicId(Long topicId) {
		this.topicId = topicId;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getAvroId() {
	   return avroId;
   }

	public void setAvroId(int avroId) {
	   this.avroId = avroId;
   }

}
