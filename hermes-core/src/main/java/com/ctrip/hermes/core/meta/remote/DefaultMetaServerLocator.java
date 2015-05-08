package com.ctrip.hermes.core.meta.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.DNSUtil;

@Named(type = MetaServerLocator.class)
public class DefaultMetaServerLocator implements MetaServerLocator, Initializable {

	private static final int DEFAULT_META_SERVER_PORT = 1248;

	private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	@SuppressWarnings("unchecked")
	private AtomicReference<List<String>> m_ipPorts = new AtomicReference<List<String>>(Collections.EMPTY_LIST);

	@Override
	public List<String> getMetaServerIpPorts() {
		try {
			m_lock.readLock().lock();
			return m_ipPorts.get();
		} finally {
			m_lock.readLock().unlock();
		}
	}

	private List<String> fetchMetaServerIpPorts() {
		List<String> curIpPorts = getMetaServerIpPorts();

		boolean dnsResolved = false;
		while (!dnsResolved) {
			if (CollectionUtil.isNullOrEmpty(curIpPorts)) {
				curIpPorts = new ArrayList<String>();
				try {
					List<String> ips = DNSUtil.resolve(getMetaServerDomainName());
					for (String ip : ips) {
						curIpPorts.add(String.format("%s:%s", ip, DEFAULT_META_SERVER_PORT));
					}
					dnsResolved = true;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}

			for (String ipPort : curIpPorts) {
				try {
					List<String> result = doFetch(ipPort);
					return result;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
			}
		}

		return null;
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
		// TODO choose domain name according to current env
		return "meta.hermes.fx.ctripcorp.com";
	}

	@Override
	public void initialize() throws InitializationException {
		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		// TODO config
		b.setConnectTimeout(2000);
		b.setSocketTimeout(2000);
		m_requestConfig = b.build();

		// TODO config interval
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				List<String> newIpPorts = fetchMetaServerIpPorts();
				if (CollectionUtil.isNotEmpty(newIpPorts)) {
					try {
						m_lock.writeLock().lock();
						m_ipPorts.set(newIpPorts);
					} finally {
						m_lock.writeLock().unlock();
					}
				}
			}

		}, 0, 10, TimeUnit.SECONDS);
	}
}
