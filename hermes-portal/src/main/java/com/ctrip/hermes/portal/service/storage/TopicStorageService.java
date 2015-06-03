package com.ctrip.hermes.portal.service.storage;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.pojo.storage.StorageTopic;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.portal.service.storage.exception.TopicIsNullException;

public interface TopicStorageService {
	/**
	 * 初始化Topic,去所有的TP上执行：
	 * 1. 创建表
	 * 2. 创建已配置好的ConsumerGroup
	 */
	public boolean initTopicStorage(Topic topic)
			  throws TopicAlreadyExistsException, TopicIsNullException, StorageHandleErrorException;


	/**
	 * 查询该Topic的Storage信息
	 */
	public StorageTopic getTopicStorage(Topic topic)
			  throws TopicIsNullException, StorageHandleErrorException;

	/**
	 * drop tables
	 */
	public boolean dropTopicStorage(Topic topic) throws StorageHandleErrorException, TopicIsNullException;

	/**
	 * 针对一个TP的MySql存储增加一个partition
	 */
	public boolean addPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException, StorageHandleErrorException;

	/**
	 * 针对一个TP的MySql存储删除一个partition
	 */
	public boolean delPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException, StorageHandleErrorException;

	/**
	 * 针对已存在的Topic,到每个TP上,新增ConsumerGroup
	 * 若Topic不存在,抛TopicNotExistedException
	 */
	public boolean addConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException, TopicIsNullException;

	/**
	 * 针对已存在的Topic,到每个TP上,删除ConsumerGroup
	 * 若Topic不存在,抛TopicNotExistedException
	 */
	public boolean delConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException, TopicIsNullException;
}
