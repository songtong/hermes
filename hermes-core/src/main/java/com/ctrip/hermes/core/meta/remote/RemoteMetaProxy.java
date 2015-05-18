package com.ctrip.hermes.core.meta.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.meta.MetaProxy;
import com.google.common.base.Function;

@Named(type = MetaProxy.class, value = RemoteMetaProxy.ID)
public class RemoteMetaProxy implements MetaProxy, Initializable {

	private static final String LEASE_ID = "leaseId";

	private static final String SESSION_ID = "sessionId";

	private static final String TOPIC = "topic";

	private static final String PARTITION = "partition";

	public final static String ID = "remote";

	@Inject
	private MetaServerLocator m_metaServerLocator;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(SESSION_ID, sessionId);
		String response = post("/lease/consumer/acquire", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewConsumerLease(Tpg tpg, Lease lease, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		String response = post("/lease/consumer/renew", params, tpg);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryRenewBrokerLease(String topic, int partition, Lease lease, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(LEASE_ID, String.valueOf(lease.getId()));
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		String response = post("/lease/broker/renew", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			return null;
		}
	}

	@Override
	public LeaseAcquireResponse tryAcquireBrokerLease(String topic, int partition, String sessionId) {
		Map<String, String> params = new HashMap<>();
		params.put(SESSION_ID, sessionId);
		params.put(TOPIC, topic);
		params.put(PARTITION, Integer.toString(partition));
		String response = post("/lease/broker/acquire", params, null);
		if (response != null) {
			return JSON.parseObject(response, LeaseAcquireResponse.class);
		} else {
			return null;
		}
	}

	private String pollMetaServer(Function<String, String> fun) {
		List<String> metaServerIpPorts = m_metaServerLocator.getMetaServerIpPorts();

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

					HttpPost post = new HttpPost(uriBuilder.build());
					post.setConfig(m_requestConfig);

					HttpResponse response;
					if (payload != null) {
						post.setEntity(new StringEntity(JSON.toJSONString(payload), ContentType.APPLICATION_JSON));
					}
					response = m_httpClient.execute(post);
					if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
						return EntityUtils.toString(response.getEntity());
					} else {
						// TODO log
						System.out.println("POST ERROR " + uriBuilder.build());
						return null;
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}

			}
		});
	}

	@Override
	public void initialize() throws InitializationException {
		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		// TODO config
		b.setConnectTimeout(2000);
		b.setSocketTimeout(2000);
		m_requestConfig = b.build();
	}

}
