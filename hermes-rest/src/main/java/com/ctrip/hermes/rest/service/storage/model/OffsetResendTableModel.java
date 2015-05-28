package com.ctrip.hermes.rest.service.storage.model;

public class OffsetResendTableModel extends TableModel {

	public OffsetResendTableModel(){
		MetaModel id = new MetaModel("id", "BIGINT(11)", "NOT NULL AUTO_INCREMENT");
		MetaModel groupId = new MetaModel("group_id", "INT(30)", "NOT NULL");
		MetaModel lastScheduleData = new MetaModel("last_schedule_date", "DATETIME", "NOT NULL");
		MetaModel lastId = new MetaModel("last_id", "BIGINT(11)", "NOT NULL");
		MetaModel creattionData = new MetaModel("creation_date", "DATETIME", "NOT NULL");
		MetaModel lastModifiedData = new MetaModel("last_modified_date", "TIMESTAMP", "NOT NULL DEFAULT " +
				  "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");

		setMetaModels(id, groupId, lastScheduleData, lastId, creattionData, lastModifiedData);
		setPrimaryKey(id);
		setTableName("offset_resend");
	}

}
