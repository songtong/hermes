package com.ctrip.hermes.portal.storage.handler;

import java.util.List;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.handler.StorageHandler;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

@Named(type = StorageHandler.class)
public class MockStorageHandler implements StorageHandler {

	@Override
	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> model, String url, String user, String pwd) throws StorageHandleErrorException {
		return false;
	}

	@Override
	public void createTable(Long topicId, Integer partitionId, List<TableModel> model, String url, String user, String pwd) throws StorageHandleErrorException {

	}

	@Override
	public boolean cleanTable() throws StorageHandleErrorException {
		return false;
	}

	@Override
	public boolean validateDataModel() throws DataModelNotMatchException {
		return false;
	}

	@Override
	public void addPartition(Long topicId, Integer partitionId, TableModel model, int range, String url, String user, String pwd) throws StorageHandleErrorException {

	}

	@Override
	public void deletePartition(Long topicId, Integer partitionId, TableModel model, String url, String user, String pwd) throws StorageHandleErrorException {

	}
}
