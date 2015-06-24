package com.ctrip.hermes.metaserver.rest.resource;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseAllocator;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterTopicAssignmentHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
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

	private static final Logger log = LoggerFactory.getLogger(LeaseResource.class);

	private static final long NO_STRATEGY_DELAY_TIME_MILLIS = 20 * 1000L;

	private static final long NO_ASSIGNMENT_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long PROXY_PASS_FAIL_DELAY_TIME_MILLIS = 5 * 1000L;

	private static final long EXCEPTION_CAUGHT_DELAY_TIME_MILLIS = 5 * 1000L;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	private ConsumerLeaseAllocatorLocator m_consumerLeaseAllocatorLocator;

	private BrokerLeaseAllocator m_brokerLeaseAllocator;

	private SystemClockService m_systemClockService;

	private ClusterTopicAssignmentHolder m_topicAssignmentHolder;

	private ClusterStateHolder m_clusterStateHolder;

	private MetaServerConfig m_config;

	public LeaseResource() {
		m_consumerLeaseAllocatorLocator = PlexusComponentLocator.lookup(ConsumerLeaseAllocatorLocator.class);
		m_brokerLeaseAllocator = PlexusComponentLocator.lookup(BrokerLeaseAllocator.class);
		m_systemClockService = PlexusComponentLocator.lookup(SystemClockService.class);
		m_topicAssignmentHolder = PlexusComponentLocator.lookup(ClusterTopicAssignmentHolder.class);
		m_config = PlexusComponentLocator.lookup(MetaServerConfig.class);
		m_clusterStateHolder = PlexusComponentLocator.lookup(ClusterStateHolder.class);

		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		b.setConnectTimeout(m_config.getProxyPassConnectTimeout());
		b.setSocketTimeout(m_config.getProxyPassReadTimeout());
		m_requestConfig = b.build();
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/acquire")
	public LeaseAcquireResponse tryAcquireConsumerLease(//
	      Tpg tpg, //
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @Context HttpServletRequest req) {

		Map<String, String> params = new HashMap<>();
		params.put("sessionId", sessionId);
		params.put("host", getRemoteAddr(host, req));
		LeaseAcquireResponse leaseAcquireResponse = proxyConsumerLeaseRequestIfNecessary(tpg.getTopic(),
		      "/consumer/acquire", params, tpg);

		if (leaseAcquireResponse == null) {
			ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
			      tpg.getGroupId());
			try {
				if (leaseAllocator != null) {
					return leaseAllocator.tryAcquireLease(tpg, sessionId, getRemoteAddr(host, req), req.getRemotePort());
				} else {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
				}
			} catch (Exception e) {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
			}
		} else {
			return leaseAcquireResponse;
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("consumer/renew")
	public LeaseAcquireResponse tryRenewConsumerLease(//
	      Tpg tpg, //
	      @QueryParam("leaseId") long leaseId,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @Context HttpServletRequest req) {

		Map<String, String> params = new HashMap<>();
		params.put("sessionId", sessionId);
		params.put("leaseId", Long.toString(leaseId));
		params.put("host", getRemoteAddr(host, req));
		LeaseAcquireResponse leaseAcquireResponse = proxyConsumerLeaseRequestIfNecessary(tpg.getTopic(),
		      "/consumer/renew", params, tpg);

		if (leaseAcquireResponse == null) {
			ConsumerLeaseAllocator leaseAllocator = m_consumerLeaseAllocatorLocator.findStrategy(tpg.getTopic(),
			      tpg.getGroupId());
			try {
				if (leaseAllocator != null) {
					return leaseAllocator.tryRenewLease(tpg, sessionId, leaseId, getRemoteAddr(host, req),
					      req.getRemotePort());
				} else {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_STRATEGY_DELAY_TIME_MILLIS);
				}
			} catch (Exception e) {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
			}
		} else {
			return leaseAcquireResponse;
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/acquire")
	public LeaseAcquireResponse tryAcquireBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("brokerPort") int port, //
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @Context HttpServletRequest req) {

		Map<String, String> params = new HashMap<>();
		params.put("topic", topic);
		params.put("partition", Integer.toString(partition));
		params.put("sessionId", sessionId);
		params.put("brokerPort", Integer.toString(port));
		params.put("host", getRemoteAddr(host, req));
		LeaseAcquireResponse leaseAcquireResponse = proxyBrokerLeaseRequestIfNecessary("/broker/acquire", params, null);

		if (leaseAcquireResponse == null) {
			try {
				return m_brokerLeaseAllocator.tryAcquireLease(topic, partition, sessionId, getRemoteAddr(host, req), port);
			} catch (Exception e) {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
			}
		} else {
			return leaseAcquireResponse;
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("broker/renew")
	public LeaseAcquireResponse tryRenewBrokerLease(//
	      @QueryParam("topic") String topic,//
	      @QueryParam("partition") int partition, //
	      @QueryParam("leaseId") long leaseId,//
	      @QueryParam("sessionId") String sessionId,//
	      @QueryParam("brokerPort") int port,//
	      @QueryParam("host") @DefaultValue("-") String host,//
	      @Context HttpServletRequest req) {

		Map<String, String> params = new HashMap<>();
		params.put("topic", topic);
		params.put("partition", Integer.toString(partition));
		params.put("leaseId", Long.toString(leaseId));
		params.put("sessionId", sessionId);
		params.put("brokerPort", Integer.toString(port));
		params.put("host", getRemoteAddr(host, req));
		LeaseAcquireResponse leaseAcquireResponse = proxyBrokerLeaseRequestIfNecessary("/broker/renew", params, null);

		if (leaseAcquireResponse == null) {
			try {
				return m_brokerLeaseAllocator.tryRenewLease(topic, partition, sessionId, leaseId, getRemoteAddr(host, req),
				      port);
			} catch (Exception e) {
				return new LeaseAcquireResponse(false, null, m_systemClockService.now()
				      + EXCEPTION_CAUGHT_DELAY_TIME_MILLIS);
			}
		} else {
			return leaseAcquireResponse;
		}
	}

	private LeaseAcquireResponse proxyBrokerLeaseRequestIfNecessary(String uri, Map<String, String> params,
	      Object payload) {
		if (m_clusterStateHolder.hasLeadership()) {
			return null;
		} else {
			HostPort leader = m_clusterStateHolder.getLeader();
			return proxyPass(leader.getHost(), leader.getPort(), uri, params, payload);
		}
	}

	private LeaseAcquireResponse proxyConsumerLeaseRequestIfNecessary(String topic, String uri,
	      Map<String, String> params, Object payload) {
		// TopicAssignmentResult assignment = m_topicAssignmentHolder.findAssignment(topic);
		//
		// if (assignment != null) {
		// if (assignment.isAssignToMe()) {
		// return null;
		// } else {
		// String host = assignment.getResponsorHost();
		// int port = assignment.getResponsorPort();
		//
		// if (!StringUtils.isBlank(host) && port > 0) {
		// if (log.isDebugEnabled()) {
		// log.debug("Proxy passing to http://{}:{}{}.(status={}}).", host, port, uri);
		// }
		// return proxyPass(host, port, uri, params, payload);
		// }
		// }
		// }
		//
		// return new LeaseAcquireResponse(false, null, m_systemClockService.now() + NO_ASSIGNMENT_DELAY_TIME_MILLIS);
		// TODO
		return null;
	}

	private LeaseAcquireResponse proxyPass(String host, int port, String uri, Map<String, String> params, Object payload) {
		uri = "/lease" + uri;
		if (log.isDebugEnabled()) {
			log.debug("Proxy pass request to http://{}:{}/{}(params={}, payload={})", host, port, uri, params,
			      JSON.toJSONString(payload));
		}
		HttpPost post = null;
		try {
			URIBuilder uriBuilder = new URIBuilder()//
			      .setScheme("http")//
			      .setHost(host)//
			      .setPort(port)//
			      .setPath(uri);
			if (params != null) {
				for (Map.Entry<String, String> entry : params.entrySet()) {
					uriBuilder.addParameter(entry.getKey(), entry.getValue());
				}
			}

			post = new HttpPost(uriBuilder.build());
			post.setConfig(m_requestConfig);

			HttpResponse response;
			if (payload != null) {
				post.setEntity(new StringEntity(JSON.toJSONString(payload), ContentType.APPLICATION_JSON));
			}
			response = m_httpClient.execute(post);
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String responseContent = EntityUtils.toString(response.getEntity());
				if (!StringUtils.isBlank(responseContent)) {
					return JSON.parseObject(responseContent, LeaseAcquireResponse.class);
				} else {
					return new LeaseAcquireResponse(false, null, m_systemClockService.now()
					      + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Response error while proxy passing to http://{}:{}{}.(status={}}).", host, port, uri,
					      response.getStatusLine().getStatusCode());
				}
				return new LeaseAcquireResponse(false, null, m_systemClockService.now() + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
			}
		} catch (Exception e) {
			// ignore
			if (log.isDebugEnabled()) {
				log.debug("Failed to proxy pass to http://{}:{}{}.", host, port, uri, e);
			}
			return new LeaseAcquireResponse(false, null, m_systemClockService.now() + PROXY_PASS_FAIL_DELAY_TIME_MILLIS);
		} finally {
			if (post != null) {
				post.reset();
			}
		}

	}

	private String getRemoteAddr(String host, HttpServletRequest req) {
		return "-".equals(host) ? req.getRemoteAddr() : host;
	}
}
