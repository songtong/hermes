package com.ctrip.hermes.metaservice.schemaregistry;

public class SchemaKey extends SchemaRegistryKey {

	private static final int MAGIC_BYTE = 0;

	private String subject;

	private Integer version;

	public SchemaKey() {

	}

	public SchemaKey(String subject, int version) {
		this.setKeytype(SchemaRegistryKeyType.SCHEMA);
		this.magic = MAGIC_BYTE;
		this.subject = subject;
		this.version = version;
	}

	public String getSubject() {
		return this.subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public int getVersion() {
		return this.version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o)) {
			return false;
		}

		SchemaKey that = (SchemaKey) o;
		if (!subject.equals(that.subject)) {
			return false;
		}
		if (version != that.version) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + subject.hashCode();
		result = 31 * result + version;
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{magic=" + this.magic + ",");
		sb.append("keytype=" + this.keytype.keytype + ",");
		sb.append("subject=" + this.subject + ",");
		sb.append("version=" + this.version + "}");
		return sb.toString();
	}

	@Override
	public int compareTo(SchemaRegistryKey o) {
		int compare = super.compareTo(o);
		if (compare == 0) {
			SchemaKey otherKey = (SchemaKey) o;
			int subjectComp = this.subject.compareTo(otherKey.subject);
			return subjectComp == 0 ? this.version - otherKey.version : subjectComp;
		} else {
			return compare;
		}
	}
}
