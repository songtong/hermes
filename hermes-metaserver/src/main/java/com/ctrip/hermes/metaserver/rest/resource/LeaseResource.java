package com.ctrip.hermes.metaserver.rest.resource;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseAllocator;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseAllocator;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseAllocatorLocator;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Path("/lease/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class LeaseResource {

	private static final long NO_STRATEGY_DELAY_TIME_MILLIS = 20 * 1000L;

	private ConsumerLeaseAllocatorLocator m_consumerLeaseAllocatorLocator = PlexusComponentLocator
	      .lookup(ConsumerLeaseAllocatorLocator.class);

	private BrokerLeaseAllocator m_brokerLeaseAllocator = PlexusComponentLocator.lookup(BrokerLeaseAllocator.class);

	private SystemClockService m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/acquire")
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, @QueryParam("sessionId") String sessionId,
	      @Context HttpServletRequest req) {
		ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
		      tpg.getGroupId());
		if (leaseAllocator != null) {
			return leaseAllocator.tryAcquireLease(tpg, sessionId, req.getRemoteAddr(), req.getRemotePort());
		} else {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/renew")
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, @QueryParam("leaseId") long leaseId,
	      @QueryParam("sessionId") String sessionId, @Context HttpServletRequest req) {
		ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
		      tpg.getGroupId());
		if (leaseAllocator != null) {
			return leaseAllocator.tryRenewLease(tpg, sessionId, leaseId, req.getRemoteAddr(), req.getRemotePort());
		} else {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
		}

	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/acquire")
	public LeaseAcquireResponse tryAcquireBrokerLease(@QueryParam("topic") String topic,
	      @QueryParam("partition") int partition, @QueryParam("sessionId") String sessionId,
	      @QueryParam("brokerPort") int port, @Context HttpServletRequest req) {
		return m_brokerLeaseAllocator.tryAcquireLease(topic, partition, sessionId, req.getRemoteAddr(), port);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/renew")
	public LeaseAcquireResponse tryRenewBrokerLease(@QueryParam("topic") String topic,
	      @QueryParam("partition") int partition, @QueryParam("leaseId") long leaseId,
	      @QueryParam("sessionId") String sessionId, @QueryParam("brokerPort") int port, @Context HttpServletRequest req) {

		return m_brokerLeaseAllocator.tryRenewLease(topic, partition, sessionId, leaseId, req.getRemoteAddr(), port);
	}

}
