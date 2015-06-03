package com.ctrip.hermes.metaserver.consumer;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.metaserver.build.BuildConstants;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseAllocator.class, value = BuildConstants.LEASE_ALLOCATOR_ORDERED_CONSUME)
public class OrderedConsumeConsumerLeaseAllocator extends AbstractConsumerLeaseAllocator {

	private static final Logger log = LoggerFactory.getLogger(OrderedConsumeConsumerLeaseAllocator.class);

	@Override
	protected LeaseAcquireResponse doAcquireLease(Tpg tpg, String consumerName, Map<String, Lease> existingValidLeases) {
		if (existingValidLeases.isEmpty()) {
			Lease newLease = m_leaseHolder.newLease(m_config.getConsumerLeaseTimeMillis());
			existingValidLeases.put(consumerName, newLease);

			log.info("Acquire lease success(topic={}, partition={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
			      tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), consumerName, newLease.getExpireTime());

			return new LeaseAcquireResponse(true, new DefaultLease(newLease.getId(), newLease.getExpireTime()
			      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1);
		} else {
			Lease existingLease = null;

			for (Map.Entry<String, Lease> entry : existingValidLeases.entrySet()) {
				Lease lease = entry.getValue();
				String leaseConsumerName = entry.getKey();
				if (leaseConsumerName.equals(consumerName)) {
					existingLease = lease;
					break;
				}
			}

			if (existingLease != null) {
				return new LeaseAcquireResponse(true, new DefaultLease(existingLease.getId(), existingLease.getExpireTime()
				      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1);
			} else {
				Collection<Lease> leases = existingValidLeases.values();
				// use the first lease's exp time
				return new LeaseAcquireResponse(false, null, leases.iterator().next().getExpireTime());
			}
		}
	}

	@Override
	protected LeaseAcquireResponse doRenewLease(Tpg tpg, String consumerName, long leaseId,
	      Map<String, Lease> existingValidLeases) {
		if (existingValidLeases.isEmpty()) {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now()
			      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
		} else {
			Lease existingLease = null;

			for (Map.Entry<String, Lease> entry : existingValidLeases.entrySet()) {
				Lease lease = entry.getValue();
				String leaseConsumerName = entry.getKey();
				if (lease.getId() == leaseId && leaseConsumerName.equals(consumerName)) {
					existingLease = lease;
					break;
				}
			}

			if (existingLease != null) {
				existingLease.setExpireTime(existingLease.getExpireTime() + m_config.getConsumerLeaseTimeMillis());
				log.info(
				      "Renew lease success(topic={}, partition={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
				      tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), consumerName, existingLease.getExpireTime());

				return new LeaseAcquireResponse(true, new DefaultLease(leaseId, existingLease.getExpireTime()
				      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1L);
			} else {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
			}
		}
	}

}