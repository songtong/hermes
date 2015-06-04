package com.ctrip.hermes.portal.service.storage.handler;

import java.util.List;

import com.ctrip.hermes.portal.pojo.storage.StorageTable;
import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

public interface StorageHandler {

	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> model,
									  String url, String user, String pwd) throws StorageHandleErrorException;

	public void createTable(Long topicId, Integer partitionId, List<TableModel> model,
									String url, String user, String pwd) throws StorageHandleErrorException;

	public boolean cleanTable() throws StorageHandleErrorException;


	public boolean validateDataModel() throws DataModelNotMatchException;

	public void addPartition(Long topicId, Integer partitionId, TableModel model, int range,
							String url, String user, String pwd) throws StorageHandleErrorException;

	public void deletePartition(Long topicId, Integer partitionId, TableModel model,
								String url, String user, String pwd) throws StorageHandleErrorException;

	public List<StorageTable> queryTable(Long topicId, Integer partitionId, String url, String user, String pwd) throws StorageHandleErrorException;
}
