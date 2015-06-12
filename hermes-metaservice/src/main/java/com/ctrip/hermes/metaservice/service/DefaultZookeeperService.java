package com.ctrip.hermes.metaservice.service;

import java.util.List;

import org.apache.curator.utils.EnsurePath;
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
	public void deleteConsumerLeaseZkPath(Topic topic) {
		List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic);

		for (String path : paths) {
			try {
				m_zkClient.getClient().delete().forPath(path);
			} catch (Exception e) {
				log.error("Exception occured in deleteConsumerLeaseZkPath", e);
				throw new RuntimeException(e);
			}
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

}
