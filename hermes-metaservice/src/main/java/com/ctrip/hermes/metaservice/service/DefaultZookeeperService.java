package com.ctrip.hermes.metaservice.service;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.service.SystemClockService;
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

	@Inject
	private SystemClockService m_systemClockService;

	public void setZkClient(ZKClient zkClient) {
		m_zkClient = zkClient;
	}

	public void setSystemClockService(SystemClockService systemClockService) {
		m_systemClockService = systemClockService;
	}

	@Override
	public void ensureConsumerLeaseZkPath(Topic topic) {

		try {
			ensurePath(ZKPathUtils.getConsumerLeaseRootZkPath());
			List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic);

			for (String path : paths) {
				ensurePath(path);
			}
		} catch (Exception e) {
			log.error("Exception occurred in ensureConsumerLeaseZkPath", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void updateZkBaseMetaVersion(long version) throws Exception {
		ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());

		m_zkClient.get().setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(version));
	}

	@Override
	public void deleteConsumerLeaseTopicParentZkPath(String topicName) {
		String path = ZKPathUtils.getConsumerLeaseTopicParentZkPath(topicName);

		try {
			deleteChildren(path, true);
		} catch (Exception e) {
			log.error("Exception occurred in deleteConsumerLeaseZkPath", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteConsumerLeaseZkPath(Topic topic, String consumerGroupName) {
		List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic, consumerGroupName);
		String topicParentPath = ZKPathUtils.getConsumerLeaseTopicParentZkPath(topic.getName());

		long now = m_systemClockService.now();

		for (String path : paths) {
			try {
				m_zkClient.get().inTransaction()//
				      .delete().forPath(path)//
				      .and().setData().forPath(topicParentPath, ZKSerializeUtils.serialize(now))//
				      .and().commit();
			} catch (Exception e) {
				log.error("Exception occurred in deleteConsumerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
		}

	}

	private void deleteChildren(String path, boolean deleteSelf) throws Exception {
		PathUtils.validatePath(path);

		CuratorFramework client = m_zkClient.get();
		Stat stat = client.checkExists().forPath(path);
		if (stat != null) {
			List<String> children = client.getChildren().forPath(path);
			for (String child : children) {
				String fullPath = ZKPaths.makePath(path, child);
				deleteChildren(fullPath, true);
			}

			if (deleteSelf) {
				try {
					client.delete().forPath(path);
				} catch (KeeperException.NotEmptyException e) {
					// someone has created a new child since we checked ... delete again.
					deleteChildren(path, true);
				} catch (KeeperException.NoNodeException e) {
					// ignore... someone else has deleted the node it since we checked
				}

			}
		}
	}

	@Override
	public void ensureBrokerLeaseZkPath(Topic topic) {
		List<String> paths = ZKPathUtils.getBrokerLeaseZkPaths(topic);

		for (String path : paths) {
			try {
				ensurePath(path);
			} catch (Exception e) {
				log.error("Exception occurred in ensureBrokerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public void deleteBrokerLeaseTopicParentZkPath(String topicName) {
		String path = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topicName);

		try {
			deleteChildren(path, true);
		} catch (Exception e) {
			log.error("Exception occurred in deleteConsumerLeaseZkPath", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public String queryData(String path) throws Exception {
		try {
			ensurePath(path);
			return ZKSerializeUtils.deserialize(m_zkClient.get().getData().forPath(path), String.class);
		} catch (Exception e) {
			log.error("Query zookeeper data failed:{}", path, e);
			throw e;
		}
	}

	@Override
	public void persist(String path, byte[] data, String... touchPaths) throws Exception {
		try {
			ensurePath(path);

			if (touchPaths != null && touchPaths.length > 0) {
				for (String touchPath : touchPaths) {
					ensurePath(touchPath);
				}
			}

			CuratorTransactionBridge curatorTransactionBridge = m_zkClient.get().inTransaction().setData()
			      .forPath(path, data);

			long now = m_systemClockService.now();
			if (touchPaths != null && touchPaths.length > 0) {
				for (String touchPath : touchPaths) {
					curatorTransactionBridge.and().setData().forPath(touchPath, ZKSerializeUtils.serialize(now));
				}
			}

			curatorTransactionBridge.and().commit();

		} catch (Exception e) {
			log.error("Exception occurred in persist", e);
			throw e;
		}
	}

	public void ensurePath(String path) throws Exception {
		EnsurePath ensurePath = m_zkClient.get().newNamespaceAwareEnsurePath(path);
		ensurePath.ensure(m_zkClient.get().getZookeeperClient());
	}

	@Override
	public void deleteMetaServerAssignmentZkPath(String topicName) {
		long now = m_systemClockService.now();

		try {
			ensurePath(ZKPathUtils.getMetaServerAssignmentZkPath(topicName));

			m_zkClient.get().inTransaction()
			      //
			      .delete().forPath(ZKPathUtils.getMetaServerAssignmentZkPath(topicName))
			      //
			      .and().setData()
			      .forPath(ZKPathUtils.getMetaServerAssignmentRootZkPath(), ZKSerializeUtils.serialize(now))//
			      .and().commit();
		} catch (Exception e) {
			log.error("Exception occurred in deleteMetaServerAssignmentZkPath", e);
			throw new RuntimeException(e);
		}
	}

}
