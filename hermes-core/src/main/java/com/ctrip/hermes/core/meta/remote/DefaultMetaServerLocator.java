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

	private static final int DEFAULT_MASTER_METASERVER_PORT = 80;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private CoreConfig m_coreConfig;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	private AtomicReference<List<String>> m_ipPorts = new AtomicReference<List<String>>(new LinkedList<String>());

	private int m_masterMetaServerPort = DEFAULT_MASTER_METASERVER_PORT;

	@Override
	public List<String> getMetaServerIpPorts() {
		return m_ipPorts.get();
	}

	private List<String> fetchMetaServerIpPorts() {
		List<String> curIpPorts = getMetaServerIpPorts();

		if (CollectionUtil.isNullOrEmpty(curIpPorts)) {
			if (!resolveMetaServerDomain(curIpPorts)) {
				throw new RuntimeException("Can not resolve meta server domain");
			}
		}

		return fetchIpPortsFromExistingMetaServer(curIpPorts);

	}

	private List<String> fetchIpPortsFromExistingMetaServer(List<String> curIpPorts) {
		for (String ipPort : curIpPorts) {
			try {
				List<String> result = doFetch(ipPort);
				return result;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("Fetch meta server ip list failed");
				continue;
			}
		}
		throw new RuntimeException("Can not fetch meta server ip list");
	}

	private boolean resolveMetaServerDomain(List<String> curIpPorts) {
		String domain = getMetaServerDomainName();
		// TODO
		System.out.println("Meta server domain " + domain);
		try {
			List<String> ips = DNSUtil.resolve(domain);
			for (String ip : ips) {
				curIpPorts.add(String.format("%s:%s", ip, m_masterMetaServerPort));
			}
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(String.format("Can not resolve meta server domain %s", domain));
			return false;
		}
	}

	private List<String> doFetch(String ipPort) throws IOException {
		String url = String.format("http://%s%s", ipPort, "/metaserver");
		HttpGet post = new HttpGet(url);
		post.setConfig(m_requestConfig);

		HttpResponse response;
		response = m_httpClient.execute(post);
		return Arrays.asList(JSON.parseArray(EntityUtils.toString(response.getEntity())).toArray(new String[0]));
	}

	private String getMetaServerDomainName() {
		Env env = m_clientEnv.getEnv();

		switch (env) {
		case DEV:
			return "127.0.0.1";
		case FWS:
			return "10.3.8.63";
		case UAT:
			return "10.2.7.72";
		case LPT:
			return "10.2.7.72";
		case PROD:
			return "meta.hermes.fx.ctripcorp.com";

		default:
			throw new IllegalArgumentException(String.format("Unknown hermes env %s", env));
		}

	}

	@Override
	public void initialize() throws InitializationException {
		m_masterMetaServerPort = Integer.parseInt(m_clientEnv.getGlobalConfig().getProperty("meta-port", "80").trim());

		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		// TODO config
		b.setConnectTimeout(2000);
		b.setSocketTimeout(2000);
		m_requestConfig = b.build();

		m_ipPorts.set(fetchMetaServerIpPorts());

		// TODO config interval
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MetaServerIpFetcher", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      m_ipPorts.set(fetchMetaServerIpPorts());
				      } catch (RuntimeException e) {
					      // TODO
					      e.printStackTrace();
				      }
			      }

		      }, 0, m_coreConfig.getMetaServerIpFetchInterval(), TimeUnit.SECONDS);
	}
}
