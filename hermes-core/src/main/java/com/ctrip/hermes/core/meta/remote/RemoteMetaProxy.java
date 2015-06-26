package com.ctrip.hermes.core.meta.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.bo.SubscriptionView;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.google.common.base.Function;

@Named(type = MetaProxy.class, value = RemoteMetaProxy.ID)
public class RemoteMetaProxy implements MetaProxy {

	private static final String HOST = "host";

	private static final String BROKER_PORT = "brokerPort";

	private static final Logger log = LoggerFactory.getLogger(RemoteMetaProxy.class);

	private static final String LEASE_ID = "leaseId";

	private static final String SESSION_ID = "sessionId";

	private static final String TOPIC = "topic";

	private static final String PARTITION = "partition";

	public final static String ID = "remote";

	@Inject
	private MetaServerLocator m_metaServerLocator;

	@Inject
	private CoreConfig m_config;

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(SESSION_ID, sessionId);
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		String response = post("/lease/consumer/acquire", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryAcquireConsumerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		String response = post("/lease/consumer/renew", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryRenewConsumerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId,
	      int brokerPort) {
		Map<String, String> params = new HashMap<>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		params.put(BROKER_PORT, String.valueOf(brokerPort));
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		String response = post("/lease/broker/renew", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryRenewBrokerLease]");
			}
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId, int brokerPort) {
		Map<String, String> params = new HashMap<>();
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		params.put(BROKER_PORT, String.valueOf(brokerPort));
		params.put(HOST, Networks.forIp().getLocalHostAddress());
		String response = post("/lease/broker/acquire", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while posting meta server[tryAcquireBrokerLease]");
			}
			return null;
		}
	}

	private String pollMetaServer(Function<String, String> fun) {
		List<String> metaServerIpPorts = m_metaServerLocator.getMetaServerList();

		for (String ipPort : metaServerIpPorts) {
			String result = fun.apply(ipPort);
			if (result != null) {
				return result;
			} else {
				continue;
			}
		}

		return null;

	}

	private String get(final String path, final Map<String, String> requestParams) {
		return pollMetaServer(new Function<String, String>() {

			@Override
			public String apply(String ip) {
				try {
					URIBuilder uriBuilder = new URIBuilder()//
					      .setScheme("http")//
					      .setHost(ip)//
					      .setPath(path);
					if (requestParams != null) {
						for (Map.Entry<String, String> entry : requestParams.entrySet()) {
							uriBuilder.addParameter(entry.getKey(), entry.getValue());
						}
					}

					HttpResponse response = Request.Get(uriBuilder.build())//
					      .connectTimeout(m_config.getMetaServerConnectTimeout())//
					      .socketTimeout(m_config.getMetaServerReadTimeout())//
					      .execute()//
					      .returnResponse();

					if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
						return EntityUtils.toString(response.getEntity());
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Response error while getting meta server error({url={}, status={}}).",
							      uriBuilder.build(), response.getStatusLine().getStatusCode());
						}
						return null;
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Get meta server error.", e);
					}
					return null;
				}

			}
		});
	}

	private String post(final String path, final Map<String, String> requestParams, final Object payload) {
		return pollMetaServer(new Function<String, String>() {

			@Override
			public String apply(String ip) {
				try {
					URIBuilder uriBuilder = new URIBuilder()//
					      .setScheme("http")//
					      .setHost(ip)//
					      .setPath(path);
					if (requestParams != null) {
						for (Map.Entry<String, String> entry : requestParams.entrySet()) {
							uriBuilder.addParameter(entry.getKey(), entry.getValue());
						}
					}

					HttpResponse response = null;

					if (payload != null) {
						response = Request.Post(uriBuilder.build())//
						      .connectTimeout(m_config.getMetaServerConnectTimeout())//
						      .socketTimeout(m_config.getMetaServerReadTimeout())//
						      .bodyString(JSON.toJSONString(payload), ContentType.APPLICATION_JSON)//
						      .execute()//
						      .returnResponse();
					} else {
						response = Request.Post(uriBuilder.build())//
						      .connectTimeout(m_config.getMetaServerConnectTimeout())//
						      .socketTimeout(m_config.getMetaServerReadTimeout())//
						      .execute()//
						      .returnResponse();
					}

					if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
						return EntityUtils.toString(response.getEntity());
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Response error while posting meta server error({url={}, status={}}).",
							      uriBuilder.build(), response.getStatusLine().getStatusCode());
						}
						return null;
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Post meta server error.", e);
					}
					return null;
				}
			}
		});
	}

	@Override
	public List<SchemaView> listSchemas() {
		String response = get("/schemas/", null);
		if (response != null) {
			return JSON.parseArray(response, SchemaView.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while getting meta server[listSchemas]");
			}
			return null;
		}
	}

	@Override
	public List<SubscriptionView> listSubscriptions() {
		String response = get("/subscriptions/", null);
		if (response != null) {
			return JSON.parseArray(response, SubscriptionView.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while getting meta server[listSubscriptions]");
			}
			return null;
		}
	}

}
