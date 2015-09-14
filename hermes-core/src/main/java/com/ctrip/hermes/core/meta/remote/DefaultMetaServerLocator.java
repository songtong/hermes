package com.ctrip.hermes.core.meta.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.helper.Urls;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.DNSUtil;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.google.common.base.Charsets;

@Named(type = MetaServerLocator.class)
public class DefaultMetaServerLocator implements MetaServerLocator, Initializable {

	private static final Logger log = LoggerFactory.getLogger(DefaultMetaServerLocator.class);

	private static final int DEFAULT_MASTER_METASERVER_PORT = 80;

	@Inject
	private ClientEnvironment m_clientEnv;

	@Inject
	private CoreConfig m_coreConfig;

	private AtomicReference<List<String>> m_metaServerList = new AtomicReference<List<String>>(new ArrayList<String>());

	private int m_masterMetaServerPort = DEFAULT_MASTER_METASERVER_PORT;

	@Override
	public List<String> getMetaServerList() {
		return m_metaServerList.get();
	}

	private void updateMetaServerList() {
		int maxTries = 10;
		RuntimeException exception = null;

		for (int i = 0; i < maxTries; i++) {
			try {
				if (CollectionUtil.isNullOrEmpty(m_metaServerList.get())) {
					m_metaServerList.set(domainToIpPorts());
				}

				List<String> metaServerList = fetchMetaServerListFromExistingMetaServer();
				if (metaServerList != null && !metaServerList.isEmpty()) {
					m_metaServerList.set(metaServerList);
					return;
				}

			} catch (RuntimeException e) {
				exception = e;
			}

			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		if (exception != null) {
			log.warn("Failed to fetch meta server list for {} times", maxTries);
			throw exception;
		}
	}

	private List<String> fetchMetaServerListFromExistingMetaServer() {
		List<String> metaServerList = new ArrayList<String>(m_metaServerList.get());
		if (log.isDebugEnabled()) {
			log.debug("Start fetching meta server ip from meta servers {}", metaServerList);
		}

		Collections.shuffle(metaServerList);

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
		String domain = m_clientEnv.getMetaServerDomainName();
		log.info("Meta server domain {}", domain);
		try {
			List<String> ips = DNSUtil.resolve(domain);
			if (CollectionUtil.isNullOrEmpty(ips)) {
				throw new RuntimeException();
			}

			List<String> ipPorts = new LinkedList<String>();
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

		InputStream is = null;

		try {
			is = Urls.forIO()//
			      .connectTimeout(m_coreConfig.getMetaServerConnectTimeout())//
			      .readTimeout(m_coreConfig.getMetaServerReadTimeout())//
			      .openStream(url);

			String response = IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());
			return Arrays.asList(JSON.parseArray(response).toArray(new String[0]));
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

	@Override
	public void initialize() throws InitializationException {
		if (m_clientEnv.isLocalMode())
			return;

		m_masterMetaServerPort = Integer.parseInt(m_clientEnv.getGlobalConfig()
		      .getProperty("meta.port", String.valueOf(DEFAULT_MASTER_METASERVER_PORT)).trim());

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
