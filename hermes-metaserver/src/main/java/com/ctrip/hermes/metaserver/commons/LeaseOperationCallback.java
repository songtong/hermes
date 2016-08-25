package com.ctrip.hermes.metaserver.commons;

import java.util.Map;

import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

public interface LeaseOperationCallback {
	public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases, int version) throws Exception;
}