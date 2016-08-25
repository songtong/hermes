package com.ctrip.hermes.metaservice.service;

import java.util.Map;

import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ZookeeperService {
	public void deleteBrokerLeaseTopicParentZkPath(String topicName);

	public void deleteConsumerLeaseTopicParentZkPath(String topicName);

	public void deleteConsumerLeaseZkPath(Topic topic, String consumerGroupName);

	public void deleteMetaServerAssignmentZkPath(String topicName);

	public void ensureBrokerLeaseZkPath(Topic topic);

	public void ensureConsumerLeaseZkPath(Topic topic);

	public void ensurePath(String path) throws Exception;

	public void persist(String path, byte[] data, String... touchPaths) throws Exception;

	public boolean persistWithVersionCheck(String path, byte[] data, int version) throws Exception;

	public String queryData(String path) throws Exception;

	public void updateZkBaseMetaVersion(long version) throws Exception;

	void persistBulk(Map<String, byte[]> pathAndDatas, String... touchPaths) throws Exception;

}
