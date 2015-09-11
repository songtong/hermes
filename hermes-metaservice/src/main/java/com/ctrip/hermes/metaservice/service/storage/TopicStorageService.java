package com.ctrip.hermes.metaservice.service.storage;

import java.util.List;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTopic;

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
	 * 针对一个TP的MySql存储批量对多个表增加一个partition
	 */
	public void addPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException, StorageHandleErrorException;

	/**
	 * 针对DataSource下的一个表增加一个partition
	 */
	public void addPartitionStorage(String ds, String table, int span) throws  StorageHandleErrorException;

	/**
	 * 针对一个TP的MySql存储批量对多个表删除一个partition
	 */
	public void delPartitionStorage(Topic topic, Partition partition) throws TopicIsNullException,
			  StorageHandleErrorException;

	/**
	 * 针对DataSource下的一个表删除一个partition
	 */
	public void delPartitionStorage(String ds, String table) throws StorageHandleErrorException;

	/**
	 * 针对已存在的Topic,到每个TP上,新增ConsumerGroup
	 * 若Topic不存在,抛TopicNotExistedException
	 */
	public boolean addConsumerStorage(Topic topic, ConsumerGroup group)
			  throws StorageHandleErrorException, TopicIsNullException;

	/**
	 * 针对已存在的Topic,到每个TP上,删除ConsumerGroup
	 * 若Topic不存在,抛TopicNotExistedException
	 */
	public boolean delConsumerStorage(Topic topic, ConsumerGroup group) throws StorageHandleErrorException, TopicIsNullException;

	public Integer queryStorageSize(String ds) throws StorageHandleErrorException;

	public Integer queryStorageSize(String ds, String table) throws StorageHandleErrorException;

	public List<StorageTable> queryStorageTables(String ds) throws StorageHandleErrorException;

	public List<StoragePartition> queryTablePartitions(String ds, String table) throws StorageHandleErrorException;

	/**
	 * 针对已有的topic, 新建一个TP, 在该TP上新建表，并初始化分区
	 * @param topic
	 * @param partition
	 * @return 
	 * @throws StorageHandleErrorException 
	 * @throws TopicIsNullException 
	 */
	public boolean addPartitionForTopic(Topic topic, Partition partition) throws TopicIsNullException, StorageHandleErrorException;
}
