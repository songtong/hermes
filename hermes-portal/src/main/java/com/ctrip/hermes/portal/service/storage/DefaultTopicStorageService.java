package com.ctrip.hermes.portal.service.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.pojo.storage.TopicStorageView;
import com.ctrip.hermes.portal.service.MetaServiceWrapper;
import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.portal.service.storage.exception.TopicNotFoundException;
import com.ctrip.hermes.portal.service.storage.handler.StorageHandler;
import com.ctrip.hermes.portal.service.storage.model.DeadLetterTableModel;
import com.ctrip.hermes.portal.service.storage.model.MessageTableModel;
import com.ctrip.hermes.portal.service.storage.model.OffsetMessageTableModel;
import com.ctrip.hermes.portal.service.storage.model.OffsetResendTableModel;
import com.ctrip.hermes.portal.service.storage.model.ResendTableModel;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

@Named(type = DefaultTopicStorageService.class, value = DefaultTopicStorageService.ID)
public class DefaultTopicStorageService implements TopicStorageService {
	private static final Logger log = LoggerFactory.getLogger(DefaultTopicStorageService.class);

	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;

	@Inject
	private MetaServiceWrapper metaService;

	public DefaultTopicStorageService() {

	}

	public boolean createNewTopic(String ds, String topicName) throws TopicAlreadyExistsException,
	      StorageHandleErrorException {
		Topic topic = metaService.findTopicByName(topicName);

		if (null != topic) {
			String databaseName = mockDatabaseName(ds);
			if (validateDatabaseName(databaseName)) {
				for (Partition partition : filterPartition(ds, topic)) {
					try {
						createTables0(databaseName, topic, partition);
						addPartitionByDatabaseName0(databaseName, topic, partition);

					} catch (StorageHandleErrorException e) {
						// todo: handle ex from createTables0 or addPartition.
						System.out.println(e.getMessage());
						return false;
					}
				}
			}
		}
		return true;
	}

	// todo: mockDatabaseName => MetaService.getDatabaseNameByDataSource()
	private String mockDatabaseName(String ds) {
		if (ds.equals("ds0")) {
			return "fxhermesshard01db";
		} else {
			return null;
		}
	}

	private void createTables0(String databaseName, Topic topic, Partition partition) throws StorageHandleErrorException {
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
		handler.createTable(databaseName, topic.getId(), partition.getId(), tableModels);
	}

	public TopicStorageView getTopicStorage(String topicName, String ds) {
		return null;
	}

	public Boolean addPartition(String ds, String topicName) throws StorageHandleErrorException {
		Topic topic = metaService.findTopicByName(topicName);

		if (null != topic) {
			String databaseName = mockDatabaseName(ds);
			if (validateDatabaseName(databaseName)) {
				for (Partition partition : filterPartition(ds, topic)) {
					try {
						addPartitionByDatabaseName0(databaseName, topic, partition);

					} catch (StorageHandleErrorException e) {
						// todo: handle ex from createTables0 or addPartition.
						System.out.println(e.getMessage());
						return false;
					}
				}
			}
		}
		return true;
	}

	private void addPartitionByDatabaseName0(String databaseName, Topic topic, Partition partition)
	      throws StorageHandleErrorException {
		// 暂时只针对MessageTableModel(0), MessageTableModel(1)分partition

		handler.addPartition(databaseName, topic.getId(), partition.getId(), new MessageTableModel(0), 100 * 10000);
		handler.addPartition(databaseName, topic.getId(), partition.getId(), new MessageTableModel(1), 100 * 10000);
	}

	public Boolean deletePartition(String ds, String topicName) throws StorageHandleErrorException {
		Topic topic = metaService.findTopicByName(topicName);

		if (null != topic) {
			String databaseName = mockDatabaseName(ds);
			if (validateDatabaseName(databaseName)) {
				for (Partition partition : filterPartition(ds, topic)) {
					try {
						deletePartition0(databaseName, topic, partition);
					} catch (StorageHandleErrorException e) {
						// todo: handle ex
						System.out.println(e.getMessage());
						return false;
					}
				}
			}
		}
		return true;
	}

	private void deletePartition0(String databaseName, Topic topic, Partition partition)
	      throws StorageHandleErrorException {
		// todo: 先做备份，再做删除

		handler.deletePartition(databaseName, topic.getId(), partition.getId(), new MessageTableModel(0));
		handler.deletePartition(databaseName, topic.getId(), partition.getId(), new MessageTableModel(1));

	}

	private List<Partition> filterPartition(String ds, Topic topic) {
		List<Partition> partitions = new ArrayList<>();
		for (Partition partition : topic.getPartitions()) {
			if (partition.getWriteDatasource().equals(ds)) {
				partitions.add(partition);
			}
		}
		return partitions;
	}

	private boolean validateDatabaseName(String databaseName) {
		if (databaseName == null || databaseName.length() == 0 || databaseName.equals("")) {
			log.error("Invalided Database Name: " + databaseName);
			return false;
		}
		return true;
	}

	public boolean cleanThenCreateNewTopic(Topic topic) {
		boolean isSuccessed = false;
		return isSuccessed;
	}

	public boolean modifyTopicConfig(Topic topic) {
		boolean isSuccessed = false;
		return isSuccessed;
	}

	/**
	 * query topic infos in storage, like topic & partition, mysql/H2 instance info
	 *
	 * @param topic
	 * @return the info about this topic
	 */
	public String getTopicConfig(Topic topic) throws TopicNotFoundException {
		return null;
	}

	/**
	 * 校验给定数据模型是否与数据库中已存在的数据模型一致
	 * 
	 * @return 是否一致
	 * @throws DataModelNotMatchException
	 */
	public boolean validateDataModel() throws DataModelNotMatchException {
		return false;
	}
}
