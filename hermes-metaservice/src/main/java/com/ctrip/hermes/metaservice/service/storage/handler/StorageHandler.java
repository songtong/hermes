package com.ctrip.hermes.metaservice.service.storage.handler;

import java.util.List;

import com.ctrip.hermes.metaservice.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.model.TableModel;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;

public interface StorageHandler {

	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> model,
									  String url, String user, String pwd) throws StorageHandleErrorException;

	public void createTable(Long topicId, Integer partitionId, List<TableModel> model,
									String url, String user, String pwd) throws StorageHandleErrorException;

	public boolean cleanTable() throws StorageHandleErrorException;


	public boolean validateDataModel() throws DataModelNotMatchException;

	public void addPartition(Long topicId, Integer partitionId, TableModel model, int range,
							String url, String user, String pwd) throws StorageHandleErrorException;

	public void addPartition(String table, int range, String url, String user, String pwd) throws StorageHandleErrorException;

	public void deletePartition(Long topicId, Integer partitionId, TableModel model,
								String url, String user, String pwd) throws StorageHandleErrorException;

	public void deletePartition(String table, String url, String user, String pwd) throws StorageHandleErrorException;

	public List<StorageTable> queryTable(Long topicId, Integer partitionId, String url, String user, String pwd) throws StorageHandleErrorException;

	public List<StorageTable> queryAllTablesInDatasourceWithoutPartition(String url, String user, String pwd) throws StorageHandleErrorException;

	public Integer queryAllSizeInDatasource(String url, String user, String pwd) throws StorageHandleErrorException;

	public Integer queryTableSize(String table, String url, String user, String pwd) throws StorageHandleErrorException;

	public List<StoragePartition> queryTablePartitions(String table, String url, String user, String pwd) throws StorageHandleErrorException;
}
