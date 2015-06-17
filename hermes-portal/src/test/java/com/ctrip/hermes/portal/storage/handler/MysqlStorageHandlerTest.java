package com.ctrip.hermes.portal.storage.handler;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.service.storage.handler.StorageHandler;
import com.ctrip.hermes.metaservice.service.storage.model.DeadLetterTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.MessageTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.OffsetMessageTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.OffsetResendTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.TableModel;


public class MysqlStorageHandlerTest extends ComponentTestCase {

	@Test
	public void testCreateDatabase() throws Exception {

	}

	@Test
	public void testCreateTable() throws Exception {

	}

	@Test
	public void testBuildSqlCreateDatabase() throws Exception {

	}

	@Test
	public void testCleanTable() throws Exception {

	}

	@Test
	public void testValidateDataModel() throws Exception {

	}

	@Test
	public void testBuildSqlCreateTable() throws Exception {
		StorageHandler handler = lookup(StorageHandler.class);

		handler.createTable( 100L, 0, buildTableModel(), "jdbc://",null, null);
	}

	private List<TableModel> buildTableModel() {
		List<TableModel> tableModels = new ArrayList<>();


		tableModels.add(new DeadLetterTableModel());   	// deadletter
		tableModels.add(new MessageTableModel(0));			// message_0
		tableModels.add(new MessageTableModel(1));			// message_1
		tableModels.add(new OffsetMessageTableModel());				// offset_message
		tableModels.add(new OffsetResendTableModel());				// offset_resend

		return tableModels;
	}

	@Test
	public void addPartition() throws Exception {
		StorageHandler handler = lookup(StorageHandler.class);
		handler.addPartition(100L, 0, new MessageTableModel(0), 1000, "jdbc://",null, null);
	}
}