package com.ctrip.hermes.core.meta;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;

public interface MetaProxy {

	Lease tryAcquireConsumerLease(Tpg tpg);

}
