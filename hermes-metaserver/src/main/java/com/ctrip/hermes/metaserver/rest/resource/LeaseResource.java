package com.ctrip.hermes.metaserver.rest.resource;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
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

	private ConsumerLeaseAllocatorLocator m_consumerLeaseAllocatorLocator = PlexusComponentLocator
	      .lookup(ConsumerLeaseAllocatorLocator.class);

	private SystemClockService m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);

	private static final long NO_STRATEGY_DELAY_TIME_MILLIS = 20 * 1000L;

	private static final long BROKER_LEASE_TIME = 60 * 1000L;

	// TODO server端lease比client端延后2秒
	private static final long BROKER_LEASE_SERVER_DELAY_TIME = 2 * 1000L;

	private Map<Pair<String, Integer>, Lease> m_brokerLeases = new HashMap<>();

	private Lock m_brokerLeaseLock = new ReentrantLock();

	private Random m_random = new Random(System.currentTimeMillis());

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/acquire")
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, @QueryParam("sessionId") String sessionId) {
		ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
		      tpg.getGroupId());
		if (leaseAllocator != null) {
			return leaseAllocator.tryAcquireLease(tpg, sessionId);
		} else {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/renew")
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, @QueryParam("leaseId") long leaseId,
	      @QueryParam("sessionId") String sessionId) {
		ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
		      tpg.getGroupId());
		if (leaseAllocator != null) {
			return leaseAllocator.tryRenewLease(tpg, sessionId, leaseId);
		} else {
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
		}

	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/acquire")
	public LeaseAcquireResponse tryAcquireBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition, //
	      @QueryParam("sessionId") String sessionId//
	) {
		Pair<String, Integer> key = new Pair<>(topic, partition);
		m_brokerLeaseLock.lock();
		long now = System.currentTimeMillis();
		try {
			Lease existsLease = m_brokerLeases.get(key);
			if (existsLease == null || existsLease.isExpired()) {
				// TODO this is mock impl
				System.out.println(String.format(
				      "[%s]Try acquire broker lease success(topic=%s, partition=%s, sessionId=%s)", new Date(), topic,
				      partition, sessionId));
				long id = now;
				m_brokerLeases.put(key, new DefaultLease(id, now + BROKER_LEASE_TIME + BROKER_LEASE_SERVER_DELAY_TIME));
				return new LeaseAcquireResponse(true, new DefaultLease(id, now + BROKER_LEASE_TIME), -1L);
			} else {
				// TODO
				System.out.println(String.format("[%s]Try acquire broker lease fail(topic=%s, partition=%s, sessionId=%s)",
				      new Date(), topic, partition, sessionId));
				return new LeaseAcquireResponse(false, null, existsLease.getExpireTime());
			}
		} finally {
			m_brokerLeaseLock.unlock();
		}

	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/renew")
	public LeaseAcquireResponse tryRenewBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition, //
	      @QueryParam("leaseId") long leaseId,//
	      @QueryParam("sessionId") String sessionId//
	) {

		Pair<String, Integer> key = new Pair<>(topic, partition);
		m_brokerLeaseLock.lock();
		try {
			Lease existsLease = m_brokerLeases.get(key);
			if (m_random.nextInt(100) != 0 || existsLease == null || existsLease.getId() != leaseId) {
				// TODO
				System.out.println(String.format("[%s]Try renew broker lease fail(topic=%s, partition=%s, sessionId=%s)",
				      new Date(), topic, partition, sessionId));
				return new LeaseAcquireResponse(false, null, existsLease == null ? -1L : existsLease.getExpireTime());
			} else {
				// TODO
				System.out.println(String.format(
				      "[%s]Try renew broker lease success(topic=%s, partition=%s, sessionId=%s)", new Date(), topic,
				      partition, sessionId));
				existsLease.setExpireTime(existsLease.getExpireTime() + BROKER_LEASE_TIME + BROKER_LEASE_SERVER_DELAY_TIME);
				return new LeaseAcquireResponse(true, new DefaultLease(leaseId, existsLease.getExpireTime()
				      - BROKER_LEASE_SERVER_DELAY_TIME), -1L);
			}
		} finally {
			m_brokerLeaseLock.unlock();
		}
	}

}
