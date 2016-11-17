package com.ctrip.hermes.admin.core.schemaregistry;

public class SchemaValue implements Comparable<SchemaValue> {

	private String subject;

	private Integer version;

	private Integer id;

	private String schema;

	public SchemaValue(){
		
	}
	
	public SchemaValue(String subject, Integer version, Integer id, String schema) {
		this.subject = subject;
		this.version = version;
		this.id = id;
		this.schema = schema;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public Integer getVersion() {
		return this.version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getSchema() {
		return this.schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SchemaValue that = (SchemaValue) o;

		if (!this.subject.equals(that.subject)) {
			return false;
		}
		if (!this.version.equals(that.version)) {
			return false;
		}
		if (!this.id.equals(that.getId())) {
			return false;
		}
		if (!this.schema.equals(that.schema)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = subject.hashCode();
		result = 31 * result + version;
		result = 31 * result + id.intValue();
		result = 31 * result + schema.hashCode();
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{subject=" + this.subject + ",");
		sb.append("version=" + this.version + ",");
		sb.append("id=" + this.id + ",");
		sb.append("schema=" + this.schema + "}");
		return sb.toString();
	}

	@Override
	public int compareTo(SchemaValue that) {
		int result = this.subject.compareTo(that.subject);
		if (result != 0) {
			return result;
		}
		result = this.version - that.version;
		return result;
	}
}