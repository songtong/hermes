package com.ctrip.hermes.metaserver.broker;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerLeaseHolder.class)
public class BrokerLeaseHolder extends BaseLeaseHolder<Pair<String, Integer>> {

	@Override
	protected String convertToZkPath(Pair<String, Integer> topicPartition) {
		return ZKPathUtils.getBrokerLeaseZkPath(topicPartition.getKey(), topicPartition.getValue());
	}
}
