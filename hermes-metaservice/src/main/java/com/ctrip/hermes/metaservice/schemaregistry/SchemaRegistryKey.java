package com.ctrip.hermes.metaservice.schemaregistry;

public abstract class SchemaRegistryKey implements Comparable<SchemaRegistryKey> {

	protected int magic;

	protected SchemaRegistryKeyType keytype;

	public int getMagic() {
		return this.magic;
	}

	public void setMagic(int magicByte) {
		this.magic = magicByte;
	}

	public SchemaRegistryKeyType getKeytype() {
		return this.keytype;
	}

	public void setKeytype(SchemaRegistryKeyType keyType) {
		this.keytype = keyType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SchemaRegistryKey that = (SchemaRegistryKey) o;

		if (this.magic != that.magic) {
			return false;
		}
		if (!this.keytype.equals(that.keytype)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = 31 * this.magic;
		result = 31 * result + this.keytype.hashCode();
		return result;
	}

	@Override
	public int compareTo(SchemaRegistryKey otherKey) {
		return this.keytype.compareTo(otherKey.keytype);
	}
}