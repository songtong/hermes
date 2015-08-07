package com.ctrip.hermes.metaservice.service.storage.model;

public class ResendTableModel extends TableModel {
	int groupId;

	public ResendTableModel(int groupId) {
		MetaModel id = new MetaModel("id", "BIGINT(11)", "NOT NULL AUTO_INCREMENT");
		MetaModel producer_ip = new MetaModel("producer_ip", "VARCHAR(15)", "NOT NULL DEFAULT ''");
		MetaModel producer_id = new MetaModel("producer_id", "INT(11)", "NOT NULL");
		MetaModel ref_key = new MetaModel("ref_key", "VARCHAR(100)", "NULL DEFAULT NULL");
		MetaModel attributes = new MetaModel("attributes", "BLOB", "NULL");
		MetaModel codec_type = new MetaModel("codec_type", "VARCHAR(20)", "NULL DEFAULT ''");
		MetaModel payload = new MetaModel("payload", "MEDIUMBLOB", "NOT NULL");
		MetaModel creation_date = new MetaModel("creation_date", "DATETIME", "NOT NULL");
		MetaModel schedule_date = new MetaModel("schedule_date", "DATETIME", "NOT NULL");
		MetaModel remaining_retries = new MetaModel("remaining_retries", "TINYINT(11)", "NOT NULL");
		MetaModel priority = new MetaModel("priority", "TINYINT(4)", "NOT NULL");
		MetaModel origin_id = new MetaModel("origin_id", "BIGINT(20)", "NOT NULL");

		setMetaModels(id, producer_ip, producer_id, ref_key, attributes, codec_type, creation_date, payload,
				  schedule_date, remaining_retries, priority, origin_id);
		setPrimaryKey(id);
		setIndexKey("id_schedule_date", id.columnName, schedule_date.columnName);

		setTableName("resend_" + groupId);
	}
}
