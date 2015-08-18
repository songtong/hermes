package com.ctrip.hermes.metaserver.consumer;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseAllocator.class, value = BuildConstants.LEASE_ALLOCATOR_NON_ORDERED_CONSUME)
public class NonOrderedConsumeConsumerLeaseAllocator extends AbstractConsumerLeaseAllocator {
	private static final Logger log = LoggerFactory.getLogger(NonOrderedConsumeConsumerLeaseAllocator.class);

	@Override
	protected LeaseAcquireResponse doAcquireLease(Tpg tpg, String consumerName,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip) throws Exception {
		if (existingValidLeases.isEmpty()) {
			return newLease(tpg, consumerName, existingValidLeases, ip);
		} else {
			Lease existingLease = null;

			for (Map.Entry<String, ClientLeaseInfo> entry : existingValidLeases.entrySet()) {
				ClientLeaseInfo clientLeaseInfo = entry.getValue();
				Lease lease = clientLeaseInfo.getLease();
				String leaseConsumerName = entry.getKey();
				if (leaseConsumerName.equals(consumerName)) {
					existingLease = lease;
					break;
				}
			}

			if (existingLease != null) {
				return new LeaseAcquireResponse(true, new Lease(existingLease.getId(), existingLease.getExpireTime()
				      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1);
			} else {
				return newLease(tpg, consumerName, existingValidLeases, ip);
			}
		}
	}

	@Override
	protected LeaseAcquireResponse doRenewLease(Tpg tpg, String consumerName, long leaseId,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip) throws Exception {
		if (existingValidLeases.isEmpty()) {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now()
			      + m_config.getDefaultLeaseAcquireOrRenewRetryDelayMillis());
		} else {
			ClientLeaseInfo existingLeaseInfo = null;

			for (Map.Entry<String, ClientLeaseInfo> entry : existingValidLeases.entrySet()) {
				ClientLeaseInfo clientLeaseInfo = entry.getValue();
				Lease lease = clientLeaseInfo.getLease();
				String leaseConsumerName = entry.getKey();
				if (lease != null && lease.getId() == leaseId && leaseConsumerName.equals(consumerName)) {
					existingLeaseInfo = clientLeaseInfo;
					break;
				}
			}

			if (existingLeaseInfo != null) {
				m_leaseHolder.renewLease(tpg, consumerName, existingValidLeases, existingLeaseInfo,
				      m_config.getConsumerLeaseTimeMillis(), ip, -1);
				if (log.isDebugEnabled()) {
					log.debug(
					      "Renew lease success(topic={}, partition={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
					      tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), consumerName, existingLeaseInfo.getLease()
					            .getExpireTime());
				}

				return new LeaseAcquireResponse(true, new Lease(leaseId, existingLeaseInfo.getLease().getExpireTime()
				      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1L);
			} else {
				Collection<ClientLeaseInfo> leases = existingValidLeases.values();
				// use the first lease's exp time
				return new LeaseAcquireResponse(false, null, leases.iterator().next().getLease().getExpireTime());

			}
		}
	}

	private LeaseAcquireResponse newLease(Tpg tpg, String consumerName,
	      Map<String, ClientLeaseInfo> existingValidLeases, String ip) throws Exception {
		Lease newLease = m_leaseHolder.newLease(tpg, consumerName, existingValidLeases,
		      m_config.getConsumerLeaseTimeMillis(), ip, -1);

		if (log.isDebugEnabled()) {
			log.debug(
			      "Acquire lease success(topic={}, partition={}, consumerGroup={}, consumerName={}, leaseExpTime={}).",
			      tpg.getTopic(), tpg.getPartition(), tpg.getGroupId(), consumerName, newLease.getExpireTime());
		}

		return new LeaseAcquireResponse(true, new Lease(newLease.getId(), newLease.getExpireTime()
		      + m_config.getConsumerLeaseClientSideAdjustmentTimeMills()), -1);
	}
}
