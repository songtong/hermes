package com.ctrip.hermes.metaservice.service.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
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

@Named(type = TopicStorageService.class)
public class DefaultTopicStorageService implements TopicStorageService {
	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;

	private static final Logger log = LoggerFactory.getLogger(DefaultTopicStorageService.class);

	@Override
	public boolean initTopicStorage(Topic topic)
			throws TopicAlreadyExistsException, TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				String writeDatasource = partition.getWriteDatasource();

				try {
					createTables0(writeDatasource, topic, partition);
					addPartition0(writeDatasource, topic, partition);
				} catch (Exception e) {
					dropTopicStorage(topic);
					return false;
				}
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	/**
	 * Create tables of deadletter, message_0, message_1, offset_message,
	 * offset_resend and (resend_groupid)s for specific TP pair.
	 * 
	 * @param dataSource
	 * @param topic
	 * @param partition
	 * @throws StorageHandleErrorException
	 */
	private void createTables0(String dataSource, Topic topic, Partition partition) throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.createTable(topic.getId(), partition.getId(), tableModels, dataSource);
		writeLog("CreateTable", topic, partition, dataSource);
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
	public void addPartitionStorage(Topic topic, Partition partition)
			throws TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			String writeDatasource = partition.getWriteDatasource();

			addPartition0(writeDatasource, topic, partition);
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
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
	private void addPartition0(String dataSource, Topic topic, Partition partition) throws StorageHandleErrorException {

		handler.addPartition(topic.getId(), partition.getId(), new DeadLetterTableModel(),
				Math.max((int) (topic.getStoragePartitionSize() / 100), 10000), topic.getStoragePartitionCount(),
				dataSource);

		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(0),
				topic.getStoragePartitionSize().intValue(), topic.getStoragePartitionCount(), dataSource);
		handler.addPartition(topic.getId(), partition.getId(), new MessageTableModel(1),
				topic.getStoragePartitionSize().intValue(), topic.getStoragePartitionCount(), dataSource);

		for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
			int groupId = consumerGroup.getId();
			handler.addPartition(topic.getId(), partition.getId(), new ResendTableModel(groupId),
					(int) Math.max(topic.getResendPartitionSize(), 100), topic.getStoragePartitionCount(), dataSource);
		}

		writeLog("AddPartition", topic, partition, dataSource);
	}

	@Override
	public StorageTopic getTopicStorage(Topic topic) throws TopicIsNullException, StorageHandleErrorException {

		if (null != topic) {
			StorageTopic storageTopic = new StorageTopic(topic);
			for (Partition partition : topic.getPartitions()) {
				String writeDatasource = partition.getWriteDatasource();

				List<StorageTable> storageTables = handler.queryTable(topic.getId(), partition.getId(),
						writeDatasource);

				if (storageTables.size() > 0) {
					storageTopic.addInfo(writeDatasource, storageTables);
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
				String writeDatasource = partition.getWriteDatasource();

				try {
					deleteTables0(writeDatasource, topic, partition);
				} catch (Exception e) {
					log.error(
							String.format("Delete topic %s, partition %d failed.", topic.getName(), partition.getId()),
							e);
					continue;
				}
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void deleteTables0(String writeDatasource, Topic topic, Partition partition)
			throws StorageHandleErrorException {
		List<TableModel> tableModels = buildTableModels(topic);
		handler.dropTables(topic.getId(), partition.getId(), tableModels, writeDatasource);

		writeLog("DeleteTables", topic, partition, writeDatasource);
	}

	@Override
	public void delPartitionStorage(Topic topic, Partition partition)
			throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			String writeDatasource = partition.getWriteDatasource();

			deletePartition0(writeDatasource, topic, partition);
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public void delPartitionStorage(String datasource, String table) throws StorageHandleErrorException {
		handler.deletePartition(table, datasource);

		writeLog("delPartitionStorage", table, datasource);
	}

	private void deletePartition0(String datasource, Topic topic, Partition partition)
			throws StorageHandleErrorException {
		handler.deletePartition(topic.getId(), partition.getId(), new DeadLetterTableModel(), datasource);

		// todo: 先做备份，再做删除
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(0), datasource);
		handler.deletePartition(topic.getId(), partition.getId(), new MessageTableModel(1), datasource);

		for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
			int groupId = consumerGroup.getId();

			handler.deletePartition(topic.getId(), partition.getId(), new ResendTableModel(groupId), datasource);
		}

		writeLog("DeletePartition", topic, partition, datasource);
	}

	@Override
	public boolean addPartitionForTopic(Topic topic, Partition partition)
			throws TopicIsNullException, StorageHandleErrorException {
		if (null != topic) {
			String writeDatasource = partition.getWriteDatasource();

			try {
				createTables0(writeDatasource, topic, partition);
				addPartition0(writeDatasource, topic, partition);
			} catch (Exception e) {
				dropTopicStorage(topic);
				return false;
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	@Override
	public boolean addConsumerStorage(Topic topic, ConsumerGroup group)
			throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				String writeDatasource = partition.getWriteDatasource();

				addConsumerStorage0(writeDatasource, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
	}

	private void addConsumerStorage0(String datasource, Topic topic, Partition partition, ConsumerGroup group)
			throws StorageHandleErrorException {
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.createTable(topic.getId(), partition.getId(), tableModels, datasource);
		handler.addPartition(topic.getId(), partition.getId(), new ResendTableModel(group.getId()),
				(int) Math.max(topic.getResendPartitionSize(), 100), topic.getStoragePartitionCount(), datasource);

		writeLog("AddConsumerStorage", topic, partition, datasource);
	}

	@Override
	public boolean delConsumerStorage(Topic topic, ConsumerGroup group)
			throws StorageHandleErrorException, TopicIsNullException {
		if (null != topic) {
			for (Partition partition : topic.getPartitions()) {
				String writeDatasource = partition.getWriteDatasource();

				delConsumerStorage0(writeDatasource, topic, partition, group);
			}
			return true;
		} else {
			throw new TopicIsNullException("Topic is Null!");
		}
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
		List<TableModel> tableModels = new ArrayList<>();

		tableModels.add(new ResendTableModel(group.getId()));
		handler.dropTables(topic.getId(), partition.getId(), tableModels, datasource);
		writeLog("DelConsumerStorage", topic, partition, datasource);

	}

	private void writeLog(String method, Topic topic, Partition partition, String datasource) {
		log.info(String.format(
				"DefaultTopicStorageService: %s is done. On Topic[%s_%d], Partition[%d] on " + "DataSource[%s].",
				method, topic.getName(), topic.getId(), partition.getId(), datasource));
	}

	private void writeLog(String method, String table, String ds) {
		log.info(String.format("DefaultTopicStorageService: %s is done. On Datasource[%s], Table[%s].", method, ds,
				table));
	}

}
