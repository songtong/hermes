package com.ctrip.hermes.rest.service.storage.model;

public class OffsetMessageTableModel extends TableModel {
	public OffsetMessageTableModel(){
		MetaModel id = new MetaModel("id", "BIGINT(11)", "NOT NULL AUTO_INCREMENT");
		MetaModel priority = new MetaModel("priority", "TINYINT(11)", "NOT NULL");
		MetaModel group_id = new MetaModel("group_id", "INT(100)", "NOT NULL");
		MetaModel offset = new MetaModel("offset", "BIGINT(11)", "NOT NULL");
		MetaModel creattionData = new MetaModel("creation_date", "DATETIME", "NOT NULL");
		MetaModel lastModifiedData = new MetaModel("last_modified_date", "TIMESTAMP", "NOT NULL DEFAULT " +
				  "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");

		setMetaModels(id, priority, group_id, offset, creattionData, lastModifiedData);
		setPrimaryKey(id);
		setTableName("offset_message");
	}
}
