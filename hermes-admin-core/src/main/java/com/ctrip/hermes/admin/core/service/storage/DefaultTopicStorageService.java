package com.ctrip.hermes.admin.core.service.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.ConsumerGroup;
import com.ctrip.hermes.admin.core.model.Partition;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.admin.core.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.admin.core.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.admin.core.service.storage.handler.StorageHandler;
import com.ctrip.hermes.admin.core.service.storage.model.DeadLetterTableModel;
import com.ctrip.hermes.admin.core.service.storage.model.MessageTableModel;
import com.ctrip.hermes.admin.core.service.storage.model.OffsetMessageTableModel;
import com.ctrip.hermes.admin.core.service.storage.model.OffsetResendTableModel;
import com.ctrip.hermes.admin.core.service.storage.model.ResendTableModel;
import com.ctrip.hermes.admin.core.service.storage.model.TableModel;
import com.ctrip.hermes.admin.core.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.admin.core.service.storage.pojo.StorageTable;
import com.ctrip.hermes.admin.core.service.storage.pojo.StorageTopic;

@Named(type = TopicStorageService.class)
public class DefaultTopicStorageService implements TopicStorageService {
	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;

	private static final Logger log = LoggerFactory.getLogger(DefaultTopicStorageService.class);

	// FIXME should create all datasources in one transaction
	@Override
	public boolean initTopicStorage(Topic topic, Collection<Partition> partitions) throws TopicAlreadyExistsException,
	      TopicIsNullException, StorageHandleErrorException {
		for (Partition partition : partitions) {
			String writeDatasource = partition.getWriteDatasource();

			try {
				createTables0(writeDatasource, topic, partition, null);
				addPartition0(writeDatasource, topic, partition, null);
			} catch (Exception e) {
				log.warn("init topic storage failed", e);
				dropTopicStorage(topic, partitions, null);
				return false;
			}
		}
		return true;
	}

	/**
	 * Create tables of deadletter, message_0, message_1, offset_message, offset_resend and (resend_groupid)s for specific TP pair.
	 * 
	 * @param dataSource
	 * @param topic
	 * @param partition
	 * @throws StorageHandleErrorException
	 */
	private void createTables0(String dataSource, Topic topic, Partition partition,
	      Collection<ConsumerGroup> consumerGroups) throws StorageHandleErrorException {
		Collection<TableModel> tableModels = buildTableModels(topic, consumerGroups);
		handler.createTable(topic.getId(), partition.getId(), tableModels, dataSource);
		writeLog("CreateTable", topic, partition, dataSource);
	}

	private Collection<TableModel> buildTableModels(Topic topic, Collection<ConsumerGroup> consumerGroups) {
		Collection<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new DeadLetterTableModel()); // deadletter
		tableModels.add(new MessageTableModel(0)); // message_0
		tableModels.add(new MessageTableModel(1)); // message_1
		tableModels.add(new OffsetMessageTableModel()); // offset_message
		tableModels.add(new OffsetResendTableModel()); // offset_resend

		if (consumerGroups != null) {
			for (ConsumerGroup consumerGroup : consumerGroups) {
				int groupId = consumerGroup.getId();

				tableModels.add(new ResendTableModel(groupId)); // resend_<groupid>
			}
		}
		return tableModels;
	}

	@Override
	public void addPartitionStorage(Topic topic, Partition partition) throws StorageHandleErrorException {
		String writeDatasource = partition.getWriteDatasource();
		addPartition0(writeDatasource, topic, partition, null);
	}

	@Override
	public void addPartitionStorage(String dataSource, String table, int span) throws StorageHandleErrorException {
		handler.addPartition(table, span, 1, dataSource);

		writeLog("addPartitionStorage by Span:" + span, table, dataSource);
	}

	/**
	 * Add storage partitions for table deadletter, message_0, message_1.
	 * 
	 * @param dataSource
	 * @param topic
	 * @param partition
	 * @throws StorageHandleErrorException
	 */
	private void addPartition0(String dataSource, Topic topic, Partition partition,
	      Collection<ConsumerGroup> consumerGroups) throws StorageHandleErrorException {

		handler.addPartition(topic.getId(), partition.getId(), new DeadLetterTableModel(),
		      Math.max((int) (topic.getStoragePartitionSize() / 100), 10000), topic.getStoragePartitionCount(),
		      dataSource);

		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(0), topic.getStoragePartitionSize()
		      .intValue(), topic.getStoragePartitionCount(), dataSource);
		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(1), topic.getStoragePartitionSize()
		      .intValue(), topic.getStoragePartitionCount(), dataSource);

		if (consumerGroups != null) {
			for (ConsumerGroup consumerGroup : consumerGroups) {
				int groupId = consumerGroup.getId();
				handler.addPartition(topic.getId(), partition.getId(), new ResendTableModel(groupId),
				      (int) Math.max(topic.getResendPartitionSize(), 100), topic.getStoragePartitionCount(), dataSource);
			}
		}
		writeLog("AddPartition", topic, partition, dataSource);
	}

	@Override
	public StorageTopic getTopicStorage(Topic topic, Collection<Partition> partitions)
	      throws StorageHandleErrorException {
		StorageTopic storageTopic = new StorageTopic(topic);
		if (partitions != null) {
			for (Partition partition : partitions) {
				String writeDatasource = partition.getWriteDatasource();

				Collection<StorageTable> storageTables = handler.queryTable(topic.getId(), partition.getId(),
				      writeDatasource);

				if (storageTables.size() > 0) {
					storageTopic.addInfo(writeDatasource, storageTables);
				}
			}
		}
		return storageTopic;
	}

	// FIXME should be in one transaction
	@Override
	public boolean dropTopicStorage(Topic topic, Collection<Partition> partitions,
	      Collection<ConsumerGroup> consumerGroups) throws StorageHandleErrorException {
		if (partitions != null) {
			for (Partition partition : partitions) {
				String writeDatasource = partition.getWriteDatasource();

				try {
					deleteTables0(writeDatasource, topic, partition, consumerGroups);
				} catch (Exception e) {
					log.error(String.format("Delete topic %s, partition %d failed.", topic.getName(), partition.getId()), e);
					continue;
				}
			}
		}
		return true;
	}

	public boolean dropTopicStorage(Topic topic, Partition partition, Collection<ConsumerGroup> consumerGroups)
	      throws StorageHandleErrorException {
		String writeDatasource = partition.getWriteDatasource();

		try {
			deleteTables0(writeDatasource, topic, partition, consumerGroups);
		} catch (Exception e) {
			log.error(String.format("Delete topic %s, partition %d failed.", topic.getName(), partition.getId()), e);
			return false;
		}
		return true;
	}

	private void deleteTables0(String writeDatasource, Topic topic, Partition partition,
	      Collection<ConsumerGroup> consumerGroups) throws StorageHandleErrorException {
		Collection<TableModel> tableModels = buildTableModels(topic, consumerGroups);
		handler.dropTables(topic.getId(), partition.getId(), tableModels, writeDatasource);

		writeLog("DeleteTables", topic, partition, writeDatasource);
	}

	@Override
	public void delPartitionStorage(Topic topic, Partition partition, Collection<ConsumerGroup> consumerGroups)
	      throws StorageHandleErrorException {
		String writeDatasource = partition.getWriteDatasource();

		deletePartition0(writeDatasource, topic, partition, consumerGroups);
	}

	@Override
	public void delPartitionStorage(String datasource, String table) throws StorageHandleErrorException {
		handler.deletePartition(table, datasource);

		writeLog("delPartitionStorage", table, datasource);
	}

	private void deletePartition0(String datasource, Topic topic, Partition partition,
	      Collection<ConsumerGroup> consumerGroups) throws StorageHandleErrorException {
		handler.deletePartition(topic.getId(), partition.getId(), new DeadLetterTableModel(), datasource);

		// todo: 先做备份，再做删除
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(0), datasource);
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(1), datasource);

		if (consumerGroups != null) {
			for (ConsumerGroup consumerGroup : consumerGroups) {
				int groupId = consumerGroup.getId();

				handler.deletePartition(topic.getId(), partition.getId(), new ResendTableModel(groupId), datasource);
			}
		}
		writeLog("DeletePartition", topic, partition, datasource);
	}

	@Override
	public boolean addPartitionForTopic(Topic topic, Partition partition, Collection<ConsumerGroup> consumerGroups)
	      throws StorageHandleErrorException {
		String writeDatasource = partition.getWriteDatasource();

		try {
			createTables0(writeDatasource, topic, partition, consumerGroups);
			addPartition0(writeDatasource, topic, partition, consumerGroups);
		} catch (Exception e) {
			dropTopicStorage(topic, partition, consumerGroups);
			return false;
		}
		return true;
	}

	@Override
	public boolean addConsumerStorage(Topic topic, Collection<Partition> partitions, ConsumerGroup group)
	      throws StorageHandleErrorException {
		if (partitions != null) {
			for (Partition partition : partitions) {
				String writeDatasource = partition.getWriteDatasource();

				addConsumerStorage0(writeDatasource, topic, partition, group);
			}
		}
		return true;
	}

	private void addConsumerStorage0(String datasource, Topic topic, Partition partition, ConsumerGroup group)
	      throws StorageHandleErrorException {
		Collection<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.createTable(topic.getId(), partition.getId(), tableModels, datasource);
		handler.addPartition(topic.getId(), partition.getId(), new ResendTableModel(group.getId()),
		      (int) Math.max(topic.getResendPartitionSize(), 100), topic.getStoragePartitionCount(), datasource);

		writeLog("AddConsumerStorage", topic, partition, datasource);
	}

	@Override
	public boolean delConsumerStorage(Topic topic, Collection<Partition> partitions, ConsumerGroup group)
	      throws StorageHandleErrorException {
		if (partitions != null) {
			for (Partition partition : partitions) {
				String writeDatasource = partition.getWriteDatasource();

				delConsumerStorage0(writeDatasource, topic, partition, group);
			}
		}
		return true;
	}

	@Override
	public Integer queryStorageSize(String datasource) throws StorageHandleErrorException {
		return handler.queryAllSizeInDatasource(datasource);
	}

	@Override
	public Integer queryStorageSize(String datasource, String table) throws StorageHandleErrorException {
		return handler.queryTableSize(table, datasource);
	}

	@Override
	public List<StorageTable> queryStorageTables(String datasource) throws StorageHandleErrorException {
		return handler.queryAllTablesInDatasourceWithoutPartition(datasource);
	}

	@Override
	public List<StoragePartition> queryTablePartitions(String datasource, String table)
	      throws StorageHandleErrorException {
		return handler.queryTablePartitions(table, datasource);
	}

	private void delConsumerStorage0(String datasource, Topic topic, Partition partition, ConsumerGroup group)
	      throws StorageHandleErrorException {
		Collection<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.dropTables(topic.getId(), partition.getId(), tableModels, datasource);
		writeLog("DelConsumerStorage", topic, partition, datasource);

	}

	private void writeLog(String method, Topic topic, Partition partition, String datasource) {
		log.info(String.format("DefaultTopicStorageService: %s is done. On Topic[%s_%d], Partition[%d] on "
		      + "DataSource[%s].", method, topic.getName(), topic.getId(), partition.getId(), datasource));
	}

	private void writeLog(String method, String table, String ds) {
		log.info(String
		      .format("DefaultTopicStorageService: %s is done. On Datasource[%s], Table[%s].", method, ds, table));
	}

}
