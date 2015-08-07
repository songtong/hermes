package com.ctrip.hermes.metaservice.service.storage.model;

public final class MessageTableModel extends TableModel{

	public MessageTableModel(int priority) {

		MetaModel id = new MetaModel("id", "BIGINT(11)", "NOT NULL AUTO_INCREMENT");
		MetaModel producer_ip = new MetaModel("producer_ip", "VARCHAR(15)", "NOT NULL DEFAULT ''");
		MetaModel producer_id = new MetaModel("producer_id", "INT(11)", "NOT NULL");
		MetaModel ref_key = new MetaModel("ref_key", "VARCHAR(100)", "NULL DEFAULT NULL");
		MetaModel attributes = new MetaModel("attributes", "BLOB", "NULL");
		MetaModel codec_type = new MetaModel("codec_type", "VARCHAR(20)", "NULL DEFAULT ''");

		MetaModel creation_date = new MetaModel("creation_date", "DATETIME", "NOT NULL");
		MetaModel payload = new MetaModel("payload", "MEDIUMBLOB", "NOT NULL");

		setMetaModels(id, producer_ip, producer_id, ref_key, attributes, codec_type, creation_date, payload);
		setPrimaryKey(id);


		setTableName("message_" + priority);
	}

}
