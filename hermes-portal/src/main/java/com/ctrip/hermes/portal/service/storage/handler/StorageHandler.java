package com.ctrip.hermes.portal.service.storage.handler;

import java.util.List;

import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

public interface StorageHandler {

	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> model,
									  String url, String user, String pwd) throws StorageHandleErrorException;

	public void createTable(Long topicId, Integer partitionId, List<TableModel> model,
									String url, String user, String pwd) throws StorageHandleErrorException;

	public boolean cleanTable() throws StorageHandleErrorException;


	/**
	 * 校验给定数据模型是否与数据库中已存在的数据模型一致
	 *
	 * @return
	 * @throws DataModelNotMatchException
	 */
	public boolean validateDataModel() throws DataModelNotMatchException;

	void addPartition(Long topicId, Integer partitionId, TableModel model, int range,
							String url, String user, String pwd) throws StorageHandleErrorException;

	void deletePartition(Long topicId, Integer partitionId, TableModel model,
								String url, String user, String pwd) throws StorageHandleErrorException;
}
