package com.ctrip.hermes.core.meta.internal;

import java.util.List;

import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

public interface MetaProxy {

	LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId);

	LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId);

	LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId, int brokerPort);

	LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort);

	List<SchemaView> listSchemas();

	List<SubscriptionView> listSubscriptions(String status);

	int registerSchema(String schema, String subject);

	String getSchemaString(int schemaId);
}
