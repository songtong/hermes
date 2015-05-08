package com.ctrip.hermes.core.meta.internal;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaProxy;

@Named(type = MetaProxy.class, value = LocalMetaProxy.ID)
public class LocalMetaProxy implements MetaProxy {

	public final static String ID = "local";

	@Override
	public Lease tryAcquireConsumerLease(Tpg tpg) {
		return new Lease(System.currentTimeMillis() + 10 * 1000);
	}

}
