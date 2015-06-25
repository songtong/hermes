package com.ctrip.hermes.metaservice.service.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.metaservice.service.storage.handler.StorageHandler;
import com.ctrip.hermes.metaservice.service.storage.model.DeadLetterTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.MessageTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.OffsetMessageTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.OffsetResendTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.ResendTableModel;
import com.ctrip.hermes.metaservice.service.storage.model.TableModel;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTopic;

@Named(type = TopicStorageService.class, value = DefaultTopicStorageService.ID)
public class DefaultTopicStorageService implements TopicStorageService {
	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;

	@Inject
	private PortalMetaService metaService;

	private static final Logger log = LoggerFactory.getLogger(DefaultTopicStorageService.class);

	@Override
	public boolean initTopicStorage(Topic topic) throws TopicAlreadyExistsException, TopicIsNullException,
	      StorageHandleErrorException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

				createTables0(dbInfo, topic, partition);

				for (int i = 0; i < 5; i++) {
					addPartition0(dbInfo, topic, partition);
				}
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void createTables0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition) throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.createTable(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(),
		      dbInfo.getLast());
		writeLog("CreateTable", topic, partition, dbInfo);
	}

	private List<TableModel> buildTableModels(Topic topic) {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new DeadLetterTableModel()); // deadletter
		tableModels.add(new MessageTableModel(0)); // message_0
		tableModels.add(new MessageTableModel(1)); // message_1
		tableModels.add(new OffsetMessageTableModel()); // offset_message
		tableModels.add(new OffsetResendTableModel()); // offset_resend

		for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
			int groupId = consumerGroup.getId();

			tableModels.add(new ResendTableModel(groupId)); // resend_<groupid>
		}
		return tableModels;
	}

	@Override
	public void addPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException,
	      StorageHandleErrorException {
		if (null != topic) {
			Datasource datasource = getDatasource(partition);
			Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

			addPartition0(dbInfo, topic, partition);
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public void addPartitionStorage(String ds, String table, int span) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

		handler.addPartition(table, span, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());

		writeLog("addPartitionStorage by Span:" + span, ds, table, dbInfo);
	}

	private void addPartition0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition) throws StorageHandleErrorException {

		handler.addPartition(topic.getId(), partition.getId(), new DeadLetterTableModel(), 1 * 10000, dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());

		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(0), 100 * 10000, dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());
		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(1), 100 * 10000, dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());

		for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
			int groupId = consumerGroup.getId();

			handler.addPartition(topic.getId(), partition.getId(), new ResendTableModel(groupId), 5 * 10000,
			      dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
		}

		writeLog("AddPartition", topic, partition, dbInfo);
	}

	@Override
	public StorageTopic getTopicStorage(Topic topic) throws TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			StorageTopic storageTopic = new StorageTopic(topic);
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

				List<StorageTable> storageTables = handler.queryTable(topic.getId(), partition.getId(), dbInfo.getFirst(),
				      dbInfo.getMiddle(), dbInfo.getLast());

				if (storageTables.size() > 0) {
					storageTopic.addInfo(datasource.getId(), storageTables);
				}
			}
			return storageTopic;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public boolean dropTopicStorage(Topic topic) throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);
				// todo: 先做备份，再做删除
				deleteTables0(dbInfo, topic, partition);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void deleteTables0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition) throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.dropTables(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(),
		      dbInfo.getLast());

		writeLog("DeleteTables", topic, partition, dbInfo);
	}

	@Override
	public void delPartitionStorage(Topic topic, Partition partition) throws StorageHandleErrorException,
	      TopicIsNullException {
		if (null != topic) {
			Datasource datasource = getDatasource(partition);
			Triple<String/* database url */, String/* usr */, String/* password */> databaseName = getDatabaseName(datasource);

			deletePartition0(databaseName, topic, partition);
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public void delPartitionStorage(String ds, String table) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);
		handler.deletePartition(table, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());

		writeLog("delPartitionStorage", ds, table, dbInfo);
	}

	private void deletePartition0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition) throws StorageHandleErrorException {
		handler.deletePartition(topic.getId(), partition.getId(), new DeadLetterTableModel(), dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());

		// todo: 先做备份，再做删除
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(0), dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(1), dbInfo.getFirst(),
		      dbInfo.getMiddle(), dbInfo.getLast());

		for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
			int groupId = consumerGroup.getId();

			handler.deletePartition(topic.getId(), partition.getId(), new ResendTableModel(groupId), dbInfo.getFirst(),
			      dbInfo.getMiddle(), dbInfo.getLast());
		}

		writeLog("DeletePartition", topic, partition, dbInfo);
	}

	@Override
	public boolean addConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException,
	      TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/* database url */, String/* usr */, String/* password */> databaseName = getDatabaseName(datasource);

				addConsumerStorage0(databaseName, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void addConsumerStorage0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition, ConsumerGroup group) throws StorageHandleErrorException {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.createTable(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(),
		      dbInfo.getLast());
		writeLog("AddConsumerStorage", topic, partition, dbInfo);
	}

	@Override
	public boolean delConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException,
	      TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);
				delConsumerStorage0(dbInfo, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public Integer queryStorageSize(String ds) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

		return handler.queryAllSizeInDatasource(dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
	}

	@Override
	public Integer queryStorageSize(String ds, String table) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

		return handler.queryTableSize(table, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
	}

	@Override
	public List<StorageTable> queryStorageTables(String ds) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

		return handler.queryAllTablesInDatasourceWithoutPartition(dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
	}

	@Override
	public List<StoragePartition> queryTablePartitions(String ds, String table) throws StorageHandleErrorException {
		Datasource datasource = getDatasource(ds);
		Triple<String/* database url */, String/* usr */, String/* password */> dbInfo = getDatabaseName(datasource);

		return handler.queryTablePartitions(table, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
	}

	private void delConsumerStorage0(Triple<String/* database url */, String/* usr */, String/* password */> dbInfo,
	      Topic topic, Partition partition, ConsumerGroup group) throws StorageHandleErrorException {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.dropTables(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(),
		      dbInfo.getLast());
		writeLog("DelConsumerStorage", topic, partition, dbInfo);

	}

	private Triple<String/* database url */, String/* usr */, String/* password */> getDatabaseName(
	      Datasource datasource) throws StorageHandleErrorException {
		String jdbcUrl = datasource.getProperties().get("url").getValue();
		String user = datasource.getProperties().get("user").getValue();
		String pwd = datasource.getProperties().get("password").getValue();
		if (null != jdbcUrl && null != user && null != pwd) {
			return new Triple<>(jdbcUrl, user, pwd);
		} else {
			throw new StorageHandleErrorException("Error Config on Datasource: " + datasource.getProperties().toString());
		}
	}

	private Datasource getDatasource(Partition partition) {
		String writeDs = partition.getWriteDatasource();
		return getDatasource(writeDs);
	}

	private Datasource getDatasource(String writeDs) {
		return metaService.findDatasource("mysql", writeDs);
	}

	private void writeLog(String method, Topic topic, Partition partition, Triple<String, String, String> dbInfo) {
		log.info(String.format("DefaultTopicStorageService: %s is done. On Topic[%s_%d], Partition[%d] on DB [%s] as "
		      + "User [%s]", method, topic.getName(), topic.getId(), partition.getId(), dbInfo.getFirst(),
		      dbInfo.getMiddle()));
	}

	private void writeLog(String method, String ds, String table, Triple<String, String, String> dbInfo) {
		log.info(String.format("DefaultTopicStorageService: %s is done. On Datasource[%s], Table[%s] on DB [%s] " +
							 "as User [%s]", method, ds, table, dbInfo.getFirst(),
				  dbInfo.getMiddle()));
	}
}
