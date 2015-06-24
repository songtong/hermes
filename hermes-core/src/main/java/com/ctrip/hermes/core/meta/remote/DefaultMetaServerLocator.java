package com.ctrip.hermes.core.meta.remote;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.Hermes.Env;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.DNSUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

@Named(type = MetaServerLocator.class)
public class DefaultMetaServerLocator implements MetaServerLocator, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaServerLocator.class);

	private static final int DEFAULT_MASTER_METASERVER_PORT = 80;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private CoreConfig m_coreConfig;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	private AtomicReference<List<String>> m_metaServerList = new AtomicReference<List<String>>(new LinkedList<String>());

	private int m_masterMetaServerPort = DEFAULT_MASTER_METASERVER_PORT;

	@Override
	public List<String> getMetaServerList() {
		return m_metaServerList.get();
	}

	private void updateMetaServerList() {
		if (CollectionUtil.isNullOrEmpty(m_metaServerList.get())) {
			m_metaServerList.set(domainToIpPorts());
		}

		m_metaServerList.set(fetchMetaServerListFromExistingMetaServer());
	}

	private List<String> fetchMetaServerListFromExistingMetaServer() {
		List<String> metaServerList = m_metaServerList.get();
		if (log.isDebugEnabled()) {
			log.debug("Start fetching meta server ip from meta servers {}", metaServerList);
		}

		for (String ipPort : metaServerList) {
			try {
				List<String> result = doFetch(ipPort);
				if (log.isDebugEnabled()) {
					log.debug("Successfully fetched meta server ip from meta server {}", ipPort);
				}
				return result;
			} catch (Exception e) {
				// ignore it
			}
		}

		throw new RuntimeException("Failed to fetch meta server ip list from any meta server: "
		      + metaServerList.toString());
	}

	private List<String> domainToIpPorts() {
		String domain = getMetaServerDomainName();
		log.info("Meta server domain {}", domain);
		try {
			List<String> ips = DNSUtil.resolve(domain);
			if (CollectionUtil.isNullOrEmpty(ips)) {
				throw new RuntimeException();
			}

			List<String> ipPorts = new LinkedList<>();
			for (String ip : ips) {
				ipPorts.add(String.format("%s:%s", ip, m_masterMetaServerPort));
			}

			return ipPorts;
		} catch (Exception e) {
			throw new RuntimeException("Can not resolve meta server domain " + domain, e);
		}
	}

	private List<String> doFetch(String ipPort) throws IOException {
		String url = String.format("http://%s%s", ipPort, "/metaserver/servers");
		HttpGet post = new HttpGet(url);
		post.setConfig(m_requestConfig);

		HttpResponse response;
		response = m_httpClient.execute(post);
		String json = EntityUtils.toString(response.getEntity());
		return Arrays.asList(JSON.parseArray(json).toArray(new String[0]));
	}

	private String getMetaServerDomainName() {
		Env env = m_clientEnv.getEnv();

		switch (env) {
		case LOCAL:
			return "127.0.0.1";
		case DEV:
			return "10.3.8.63";
		case LPT:
			return "10.3.8.63";
		case FWS:
			return "meta.hermes.fws.qa.nt.ctripcorp.com";
		case UAT:
			return "meta.hermes.fx.uat.qa.nt.ctripcorp.com";
		case PROD:
			return "meta.hermes.fx.ctripcorp.com";

		default:
			throw new IllegalArgumentException(String.format("Unknown hermes env %s", env));
		}

	}

	@Override
	public void initialize() throws InitializationException {
		if (m_clientEnv.isLocalMode())
			return;

		m_masterMetaServerPort = Integer.parseInt(m_clientEnv.getGlobalConfig()
		      .getProperty("meta.port", String.valueOf(DEFAULT_MASTER_METASERVER_PORT)).trim());

		m_httpClient = HttpClients.createDefault();
		Builder b = RequestConfig.custom();
		b.setConnectTimeout(m_coreConfig.getMetaServerConnectTimeout());
		b.setSocketTimeout(m_coreConfig.getMetaServerReadTimeout());
		m_requestConfig = b.build();

		updateMetaServerList();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MetaServerIpFetcher", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      updateMetaServerList();
				      } catch (RuntimeException e) {
					      log.warn("", e);
				      }
			      }

		      }, m_coreConfig.getMetaServerIpFetchInterval(), m_coreConfig.getMetaServerIpFetchInterval(),
		            TimeUnit.SECONDS);
	}
}
