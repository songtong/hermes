package com.ctrip.hermes.metaservice.service;

import java.util.List;

import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ZookeeperService.class)
public class DefaultZookeeperService implements ZookeeperService {
	private static final Logger log = LoggerFactory.getLogger(DefaultZookeeperService.class);

	@Inject
	private ZKClient m_zkClient;

	@Override
	public void ensureConsumerLeaseZkPath(Topic topic) {

		List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic);

		for (String path : paths) {
			try {
				EnsurePath ensurePath = m_zkClient.getClient().newNamespaceAwareEnsurePath(path);
				ensurePath.ensure(m_zkClient.getClient().getZookeeperClient());
			} catch (Exception e) {
				log.error("Exception occured in ensureConsumerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void updateZkMetaVersion(int version) throws Exception {
		EnsurePath ensurePath = m_zkClient.getClient().newNamespaceAwareEnsurePath(ZKPathUtils.getMetaVersionPath());
		ensurePath.ensure(m_zkClient.getClient().getZookeeperClient());

		m_zkClient.getClient().setData().forPath(ZKPathUtils.getMetaVersionPath(), ZKSerializeUtils.serialize(version));
	}

	@Override
	public void deleteConsumerLeaseZkPath(String topicName) {
		String path = ZKPathUtils.getConsumerLeaseZkPath(topicName);

		try {
			deleteChildren(path, true);
		} catch (Exception e) {
			log.error("Exception occured in deleteConsumerLeaseZkPath", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteConsumerLeaseZkPath(Topic topic, String consumerGroupName) {
		List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic, consumerGroupName);

		for (String path : paths) {
			try {
				m_zkClient.getClient().delete().forPath(path);
			} catch (Exception e) {
				log.error("Exception occured in deleteConsumerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
		}

	}

	private void deleteChildren(String path, boolean deleteSelf) throws Exception {
		PathUtils.validatePath(path);

		List<String> children = m_zkClient.getClient().getChildren().forPath(path);
		for (String child : children) {
			String fullPath = ZKPaths.makePath(path, child);
			deleteChildren(fullPath, true);
		}

		if (deleteSelf) {
			try {
				m_zkClient.getClient().delete().forPath(path);
			} catch (KeeperException.NotEmptyException e) {
				// someone has created a new child since we checked ... delete again.
				deleteChildren(path, true);
			} catch (KeeperException.NoNodeException e) {
				// ignore... someone else has deleted the node it since we checked
			}
		}
	}

	@Override
	public void ensureBrokerLeaseZkPath(Topic topic) {
		List<String> paths = ZKPathUtils.getBrokerLeaseZkPaths(topic);

		for (String path : paths) {
			try {
				EnsurePath ensurePath = m_zkClient.getClient().newNamespaceAwareEnsurePath(path);
				ensurePath.ensure(m_zkClient.getClient().getZookeeperClient());
			} catch (Exception e) {
				log.error("Exception occured in ensureBrokerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public void deleteBrokerLeaseZkPath(String topicName) {
		String path = ZKPathUtils.getBrokerLeaseZkPath(topicName);

		try {
			deleteChildren(path, true);
		} catch (Exception e) {
			log.error("Exception occured in deleteConsumerLeaseZkPath", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void persist(String path, Object data) throws Exception {
		try {
			EnsurePath ensurePath = m_zkClient.getClient().newNamespaceAwareEnsurePath(path);
			ensurePath.ensure(m_zkClient.getClient().getZookeeperClient());
			m_zkClient.getClient().setData().forPath(path, ZKSerializeUtils.serialize(data));
		} catch (Exception e) {
			log.error("Exception occured in persist", e);
			throw e;
		}
	}

}
