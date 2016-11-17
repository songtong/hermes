package com.ctrip.hermes.admin.core.service.storage.handler;

import java.util.Collection;
import java.util.List;

import com.ctrip.hermes.admin.core.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.admin.core.service.storage.model.TableModel;
import com.ctrip.hermes.admin.core.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.admin.core.service.storage.pojo.StorageTable;

public interface StorageHandler {

	public void dropTables(Long topicId, Integer partitionId, Collection<TableModel> model, String datasource)
	      throws StorageHandleErrorException;

	public void createTable(Long topicId, Integer partitionId, Collection<TableModel> model, String datasource)
	      throws StorageHandleErrorException;

	public void addPartition(Long topicId, Integer partitionId, TableModel model, int l, int count, String datasource)
	      throws StorageHandleErrorException;

	public void addPartition(String table, int range, int count, String datasource) throws StorageHandleErrorException;

	public void deletePartition(Long topicId, Integer partitionId, TableModel model, String datasource)
	      throws StorageHandleErrorException;

	public void deletePartition(String table, String datasource) throws StorageHandleErrorException;

	public List<StorageTable> queryTable(Long topicId, Integer partitionId, String datasource)
	      throws StorageHandleErrorException;

	public List<StorageTable> queryAllTablesInDatasourceWithoutPartition(String datasource)
	      throws StorageHandleErrorException;

	public Integer queryAllSizeInDatasource(String datasource) throws StorageHandleErrorException;

	public Integer queryTableSize(String table, String datasource) throws StorageHandleErrorException;

	public List<StoragePartition> queryTablePartitions(String table, String datasource)
	      throws StorageHandleErrorException;
}
