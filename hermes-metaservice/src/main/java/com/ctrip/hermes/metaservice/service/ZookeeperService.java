package com.ctrip.hermes.metaservice.service;

import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface ZookeeperService {
	public void ensureConsumerLeaseZkPath(Topic topic);

	public void ensureBrokerLeaseZkPath(Topic topic);

	public void deleteConsumerLeaseTopicParentZkPath(String topicName);

	public void deleteBrokerLeaseTopicParentZkPath(String topicName);

	public void deleteConsumerLeaseZkPath(Topic t, String consumerGroupName);

	public void updateZkBaseMetaVersion(long version) throws Exception;

	public void persist(String path, byte[] data, String... touchPaths) throws Exception;

	public void ensurePath(String path) throws Exception;

	public void deleteMetaServerAssignmentZkPath(String topicName);

	public String queryData(String path) throws Exception;

}
