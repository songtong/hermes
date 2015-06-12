package com.ctrip.hermes.metaserver.consumer;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {

	@Override
	protected String convertToZkPath(Tpg tpg) {
		return ZKPathUtils.getConsumerLeaseZkPath(tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
	}

}
