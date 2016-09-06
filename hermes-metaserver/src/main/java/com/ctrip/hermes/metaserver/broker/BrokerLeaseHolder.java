package com.ctrip.hermes.metaserver.broker;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	@Override
	protected String convertKeyToZkPath(Pair<String, Integer> topicPartition) {
		return ZKPathUtils.getBrokerLeaseZkPath(topicPartition.getKey(), topicPartition.getValue());
	}

	@Override
	protected Pair<String, Integer> convertZkPathToKey(String path) {
		return ZKPathUtils.parseBrokerLeaseZkPath(path);
	}

	@Override
	protected String getBaseZkPath() {
		return ZKPathUtils.getBrokerLeasesZkPath();
	}

	@Override
	protected String getName() {
		return "BrokerLeaseHolder";
	}

	@Override
	protected boolean isPathMatch(String path) {
		if (!StringUtils.isBlank(path) && path.startsWith(getBaseZkPath())) {
			try {
				return ZKPathUtils.parseBrokerLeaseZkPath(path) != null;
			} catch (RuntimeException e) {
				// ignore
			}
		}

		return false;
	}
}
