package com.ctrip.hermes.core.meta.remote;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
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
import com.google.common.base.Charsets;
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
		Map<String, String> params = new HashMap<String, String>();
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
		Map<String, String> params = new HashMap<String, String>();
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
		Map<String, String> params = new HashMap<String, String>();
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
		Map<String, String> params = new HashMap<String, String>();
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
				String url = String.format("http://%s%s", ip, path);
				InputStream is = null;
				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}

					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(m_config.getMetaServerConnectTimeout());
					conn.setReadTimeout(m_config.getMetaServerReadTimeout());
					conn.setRequestMethod("GET");
					conn.connect();

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Response error while getting meta server error({url={}, status={}}).", url, statusCode);
						}
						return null;
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Get meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
	}

	private String post(final String path, final Map<String, String> requestParams, final Object payload) {
		return pollMetaServer(new Function<String, String>() {

			@Override
			public String apply(String ip) {

				String url = String.format("http://%s%s", ip, path);
				InputStream is = null;
				OutputStream os = null;

				try {
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							url = url + "?" + encodedRequestParamStr;
						}
					}

					HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

					conn.setConnectTimeout(m_config.getMetaServerConnectTimeout());
					conn.setReadTimeout(m_config.getMetaServerReadTimeout());
					conn.setRequestMethod("POST");
					conn.addRequestProperty("content-type", "application/json");

					if (payload != null) {
						conn.setDoOutput(true);
						conn.connect();
						os = conn.getOutputStream();
						os.write(JSON.toJSONBytes(payload));
					} else {
						conn.connect();
					}

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						return IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Response error while posting meta server error({url={}, status={}}).", url, statusCode);
						}
						return null;
					}

				} catch (Exception e) {
					// ignore
					if (log.isDebugEnabled()) {
						log.debug("Post meta server error.", e);
					}
					return null;
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}

					if (os != null) {
						try {
							os.close();
						} catch (Exception e) {
							// ignore it
						}
					}
				}

			}
		});
	}

	private String encodePropertiesStr(Map<String, String> properties) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()))//
			      .append("=")//
			      .append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8.name()))//
			      .append("&");
		}

		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
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
	public List<SubscriptionView> listSubscriptions(String status) {
		String response = get("/subscriptions/" + status, null);
		if (response != null) {
			return JSON.parseArray(response, SubscriptionView.class);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No response while getting meta server[listSubscriptions]");
			}
			return null;
		}
	}

	@Override
	public int registerSchema(String schema, String subject) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("schema", schema);
		params.put("subject", subject);
		String response = post("/schema/register", null, params);
		if (response != null) {
			try {
				return Integer.valueOf(response);
			} catch (Exception e) {
				log.error("Can not parse response, schema: {}, subject: {}\nResponse: {}", schema, subject, response);
			}
		}
		throw new RuntimeException(String.format("Register schema %s[%s] failed.", subject, schema));
	}

	@Override
	public String getSchemaString(int schemaId) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("id", String.valueOf(schemaId));
		String response = get("/schema/register", params);
		if (response != null) {
			return response;
		} else {
			log.warn("No response while getting meta server[getSchemaString]");
		}
		return null;
	}
}
