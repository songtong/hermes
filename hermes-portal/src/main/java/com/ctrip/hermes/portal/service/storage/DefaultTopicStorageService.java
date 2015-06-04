package com.ctrip.hermes.portal.service.storage;

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
import com.ctrip.hermes.portal.pojo.storage.StorageTable;
import com.ctrip.hermes.portal.pojo.storage.StorageTopic;
import com.ctrip.hermes.portal.service.MetaServiceWrapper;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.portal.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.portal.service.storage.handler.StorageHandler;
import com.ctrip.hermes.portal.service.storage.model.*;

@Named(type = TopicStorageService.class, value = DefaultTopicStorageService.ID)
public class DefaultTopicStorageService implements TopicStorageService {
	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;

	@Inject
	private MetaServiceWrapper metaService;

	private static final Logger log = LoggerFactory.getLogger(DefaultTopicStorageService.class);

	@Override
	public boolean initTopicStorage(Topic topic) throws TopicAlreadyExistsException, TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo = getDatabaseName(datasource);

				createTables0(dbInfo, topic, partition);
				addPartition0(dbInfo, topic, partition);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void createTables0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
										Topic topic, Partition partition) throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.createTable(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(),
				  dbInfo.getLast());
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
	public boolean addPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			Datasource datasource = getDatasource(partition);
			Triple<String/*database url*/, String/*usr*/, String/*password*/> databaseName = getDatabaseName(datasource);

			addPartition0(databaseName, topic, partition);
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}


	private void addPartition0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
										Topic topic, Partition partition)
			  throws StorageHandleErrorException {
		// 暂时只针对MessageTableModel(0), MessageTableModel(1)分partition

		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(0), 100 *
				  10000, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(1), 100 *
				  10000, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo.getLast());
	}

	public Boolean deletePartition(String ds, String topicName) throws StorageHandleErrorException {
		//todo:
		throw new RuntimeException("Not Implemented!");
	}

	@Override
	public StorageTopic getTopicStorage(Topic topic) throws TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			StorageTopic storageTopic = new StorageTopic(topic);
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo = getDatabaseName(datasource);

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
				Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo = getDatabaseName(datasource);
				// todo: 先做备份，再做删除
				deleteTables0(dbInfo, topic, partition);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void deleteTables0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
										Topic topic, Partition partition) throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.dropTables(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo
				  .getLast());
	}

	@Override
	public boolean delPartitionStorage(Topic topic, Partition partition) throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			Datasource datasource = getDatasource(partition);
			Triple<String/*database url*/, String/*usr*/, String/*password*/> databaseName = getDatabaseName(datasource);

			deletePartition0(databaseName, topic, partition);
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void deletePartition0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
											Topic topic, Partition partition)
			  throws StorageHandleErrorException {
		// todo: 先做备份，再做删除
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(0), dbInfo.getFirst(), dbInfo
				  .getMiddle(), dbInfo.getLast());
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(1), dbInfo.getFirst(), dbInfo
				  .getMiddle(), dbInfo.getLast());
	}

	@Override
	public boolean addConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/*database url*/, String/*usr*/, String/*password*/> databaseName = getDatabaseName(datasource);

				addConsumerStorage0(databaseName, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void addConsumerStorage0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
												Topic topic, Partition partition, ConsumerGroup group)
			  throws StorageHandleErrorException {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.createTable(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo
				  .getLast());
	}

	@Override
	public boolean delConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				Datasource datasource = getDatasource(partition);
				Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo = getDatabaseName(datasource);
				delConsumerStorage0(dbInfo, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void delConsumerStorage0(Triple<String/*database url*/, String/*usr*/, String/*password*/> dbInfo,
												Topic topic, Partition partition, ConsumerGroup group) throws StorageHandleErrorException {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.dropTables(topic.getId(), partition.getId(), tableModels, dbInfo.getFirst(), dbInfo.getMiddle(), dbInfo
				  .getLast());
	}

	private Triple<String/*database url*/, String/*usr*/, String/*password*/> getDatabaseName(Datasource datasource)
			  throws StorageHandleErrorException {
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
		return metaService.getDatasource("mysql", writeDs);
	}

	private boolean validateDatabaseName(String databaseName) {
		if (databaseName == null || databaseName.length() == 0 || databaseName.equals("")) {
			log.error("Invalided Database Name: " + databaseName);
			return false;
		}
		return true;
	}
}
