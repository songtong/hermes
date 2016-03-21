package com.ctrip.hermes.core.meta.internal;

import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

public interface MetaProxy {

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId, int brokerPort);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort);

	int registerSchema(String schema, String subject);

	String getSchemaString(int schemaId);

	Map<Integer, Offset> findMessageOffsetByTime(String topic, int partition, long time);
	
	Pair<Integer, String> getRequestToMetaServer(String path, Map<String, String> requestParams);
}
