package com.ctrip.hermes.metaservice.schemaregistry;

public enum SchemaRegistryKeyType {
	  CONFIG("CONFIG"),
	  SCHEMA("SCHEMA"),
	  NOOP("NOOP");

	  public final String keytype;

	  private SchemaRegistryKeyType(String keyType) {
	    this.keytype = keyType;
	  }

	  public static SchemaRegistryKeyType forName(String keyType) {
	    if (CONFIG.keytype.equals(keyType)) {
	      return CONFIG;
	    } else if (SCHEMA.keytype.equals(keyType)) {
	      return SCHEMA;
	    } else if (NOOP.keytype.equals(keyType)) {
	      return NOOP;
	    } else {
	      throw new IllegalArgumentException("Unknown schema registry key type : " + keyType
	                                         + " Valid key types are {config, schema}");
	    }
	  }
	}
