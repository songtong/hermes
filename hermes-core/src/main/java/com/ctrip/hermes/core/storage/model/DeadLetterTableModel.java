package com.ctrip.hermes.core.storage.model;

public final class DeadLetterTableModel extends TableModel{
	public DeadLetterTableModel(){
		MetaModel id = new MetaModel("id", "BIGINT(11)", "NOT NULL AUTO_INCREMENT");
		MetaModel producer_ip = new MetaModel("producer_ip", "VARCHAR(15)", "NOT NULL DEFAULT ''");
		MetaModel producer_id = new MetaModel("producer_id", "INT(11)", "NOT NULL");
		MetaModel ref_key = new MetaModel("ref_key", "VARCHAR(100)", "NULL DEFAULT NULL");
		MetaModel attributes = new MetaModel("attributes", "BLOB", "NULL");
		MetaModel creation_date = new MetaModel("creation_date", "DATETIME", "NOT NULL");
		MetaModel payload = new MetaModel("payload", "MEDIUMBLOB", "NOT NULL");
		MetaModel dead_date = new MetaModel("dead_date", "DATETIME", "NOT NULL");
		MetaModel group_id = new MetaModel("group_id", "INT(11)", "NULL DEFAULT NULL");

		setMetaModels(id, producer_ip, producer_id, ref_key, attributes, creation_date, payload,
				  dead_date, group_id);
		setPrimaryKey(id);
		setIndexKey("key", ref_key.columnName);
		setTableName("dead_letter");
	}
}
