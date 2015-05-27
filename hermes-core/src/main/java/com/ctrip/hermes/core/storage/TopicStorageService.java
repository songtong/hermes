package com.ctrip.hermes.core.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.core.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.core.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.core.storage.exception.TopicNotFoundException;
import com.ctrip.hermes.core.storage.handler.StorageHandler;
import com.ctrip.hermes.core.storage.model.*;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = TopicStorageService.class, value = TopicStorageService.ID)
public class TopicStorageService {
	private static final Logger log = LoggerFactory.getLogger(TopicStorageService.class);
	public static final String ID = "topic-storage-service";

	@Inject
	private StorageHandler handler;
	@Inject
	private MetaService metaService;

	public TopicStorageService() {

	}

	public boolean createNewTopic(String ds, String topicName) throws TopicAlreadyExistsException {
		String databaseName = null;
		// todo: mock ds--database
		if (ds.equals("ds0")) {
			databaseName = "fxhermesshard01db";

			Topic topic = metaService.findTopic(topicName);
			if (null != topic) {
				return createNewTopic(databaseName, topic);
			}
		}
		return false;
	}

	public boolean createNewTopic(String databaseName, Topic topic) throws TopicAlreadyExistsException {

		if (!validateDatabaseName(databaseName) || ! validateTopicNotNull(topic)) {
			return false;
		}


		boolean isSucceeded = false;
		try {
			for (Partition partition : topic.getPartitions()) {
				List<TableModel> tableModels = new ArrayList<>();

				tableModels.add(new DeadLetterTableModel());   	// deadletter
				tableModels.add(new MessageTableModel(0));			// message_0
				tableModels.add(new MessageTableModel(1));			// message_1
				tableModels.add(new OffsetMessageTableModel());				// offset_message
				tableModels.add(new OffsetResendTableModel());				// offset_resend


				for (ConsumerGroup consumerGroup : topic.getConsumerGroups()) {
					int groupId = consumerGroup.getId();

					tableModels.add(new ResendTableModel(groupId));		// resend_<groupid>
				}
				;
				handler.createTable(databaseName, topic.getId(), partition.getId(),  tableModels);
			}

			isSucceeded = true;
		} catch (StorageHandleErrorException e) {
			log.error(String.format("Exception in CreateNewTopic: %s", e.getMessage()));
		}

		return isSucceeded;
	}

	private boolean validateTopicNotNull(Topic topic) {
		if (null == topic) {
			log.error("Invalided Topic: null.");
			return false;
		}
		return true;
	}

	private boolean validateDatabaseName(String databaseName) {
		if (databaseName == null || databaseName.length() == 0 || databaseName.equals("")) {
			log.error("Invalided Database Name: " + databaseName );
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
	 * @return 是否一致
	 * @throws DataModelNotMatchException
	 */
	public boolean validateDataModel() throws DataModelNotMatchException {
		return false;
	}
}
