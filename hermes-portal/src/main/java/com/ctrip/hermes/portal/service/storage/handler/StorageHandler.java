package com.ctrip.hermes.portal.service.storage.handler;

import java.util.List;

import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

public interface StorageHandler {


	public boolean dropTables() throws StorageHandleErrorException;
	public void createTable(String databaseName, Long topicId, Integer partitionId,  List<TableModel> models) throws
			  StorageHandleErrorException;

	public boolean cleanTable() throws  StorageHandleErrorException;


	/**
	 * 校验给定数据模型是否与数据库中已存在的数据模型一致
	 * @return
	 * @throws DataModelNotMatchException
	 */
	public boolean validateDataModel() throws DataModelNotMatchException;
}
