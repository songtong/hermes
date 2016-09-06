package com.ctrip.hermes.metaserver.consumer;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {

	@Override
	protected String convertKeyToZkPath(Tpg tpg) {
		return ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
	}

	@Override
	protected Tpg convertZkPathToKey(String path) {
		return ZKPathUtils.parseConsumerLeaseZkPath(path);
	}

	@Override
	protected String getName() {
		return "ConsumerLeaseHolder";
	}

	@Override
	protected String getBaseZkPath() {
		return ZKPathUtils.getConsumerLeaseRootZkPath();
	}

	@Override
	protected boolean isPathMatch(String path) {
		if (!StringUtils.isBlank(path) && path.startsWith(getBaseZkPath())) {
			try {
				return ZKPathUtils.parseConsumerLeaseZkPath(path) != null;
			} catch (RuntimeException e) {
				// ignore
			}
		}

		return false;
	}
}
