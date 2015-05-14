package com.ctrip.hermes.meta.resource;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;

@Path("/lease/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class LeaseResource {
	private static final long LEASE_TIME = 20 * 1000L;

	// TODO server端lease比client端延后2秒
	private static final long LEASE_SERVER_DELAY_TIME = 2 * 1000L;

	private Map<Tpg, Lease> m_consumerLeases = new HashMap<>();

	private Lock m_lock = new ReentrantLock();

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/acquire")
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, @QueryParam("sessionId") String sessionId) {
		m_lock.lock();
		long now = System.currentTimeMillis();
		try {
			Lease existsLease = m_consumerLeases.get(tpg);
			if (existsLease == null || existsLease.getExpireTime() < now) {
				// TODO this is mock impl
				System.out.println(String.format("[%s]Try acquire consumer lease success(tpg=%s, sessionId=%s)",
				      new Date(), tpg, sessionId));
				long id = now;
				m_consumerLeases.put(tpg, new Lease(id, now + LEASE_TIME + LEASE_SERVER_DELAY_TIME));
				return new LeaseAcquireResponse(true, new Lease(id, now + LEASE_TIME), -1L);
			} else {
				// TODO
				System.out.println(String.format("[%s]Try acquire consumer lease fail(tpg=%s, sessionId=%s)", new Date(),
				      tpg, sessionId));
				return new LeaseAcquireResponse(false, null, existsLease.getExpireTime());
			}
		} finally {
			m_lock.unlock();
		}

	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/renew")
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, @QueryParam("leaseId") long leaseId,
	      @QueryParam("sessionId") String sessionId) {
		m_lock.lock();
		try {
			Lease existsLease = m_consumerLeases.get(tpg);
			if (existsLease == null || existsLease.getId() != leaseId) {
				// TODO
				System.out.println(String.format("[%s]Try renew consumer lease fail(tpg=%s, sessionId=%s)", new Date(),
				      tpg, sessionId));
				return new LeaseAcquireResponse(false, null, -1L);
			} else {
				// TODO
				System.out.println(String.format("[%s]Try renew consumer lease success(tpg=%s, sessionId=%s)",
				      new Date(), tpg, sessionId));
				existsLease.setExpireTime(existsLease.getExpireTime() + LEASE_TIME + LEASE_SERVER_DELAY_TIME);
				return new LeaseAcquireResponse(true, new Lease(leaseId, existsLease.getExpireTime()
				      - LEASE_SERVER_DELAY_TIME), -1L);
			}
		} finally {
			m_lock.unlock();
		}
	}

}
