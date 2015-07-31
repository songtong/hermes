package com.ctrip.hermes.core.meta.remote;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.internal.MetaLoader;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	private static final Logger log = LoggerFactory.getLogger(RemoteMetaLoader.class);

	public static final String ID = "remote-meta-loader";

	@Inject
	private ClientEnvironment m_clientEnvironment;

	@Inject
	private MetaServerLocator m_metaServerLocator;

	@Inject
	private CoreConfig m_config;

	private AtomicReference<Meta> m_metaCache = new AtomicReference<Meta>(null);

	@Override
	public Meta load() {
		List<String> ipPorts = m_metaServerLocator.getMetaServerList();
		if (ipPorts == null || ipPorts.isEmpty()) {
			throw new RuntimeException("No meta server found.");
		}

		for (String ipPort : ipPorts) {
			if (log.isDebugEnabled()) {
				log.debug("Loading meta from server: {}", ipPort);
			}

			String url = null;
			try {
				String uri = m_clientEnvironment.getGlobalConfig().getProperty("meta.fetch.remote.uri");
				if (StringUtils.isBlank(uri)) {
					if (log.isDebugEnabled()) {
						log.debug("Can not find meta fetch uri, use \"/meta\" as default.");
					}
					uri = "/meta";
				}

				url = String.format("http://%s%s", ipPort, uri);
				if (m_metaCache.get() != null) {
					url += "?version=" + m_metaCache.get().getVersion();
				}

				HttpResponse response = Request.Get(url)//
				      .connectTimeout(m_config.getMetaServerConnectTimeout())//
				      .socketTimeout(m_config.getMetaServerReadTimeout())//
				      .execute()//
				      .returnResponse();

				int statusCode = response.getStatusLine().getStatusCode();

				if (statusCode == HttpStatus.SC_OK) {
					String responseContent = EntityUtils.toString(response.getEntity());
					m_metaCache.set(JSON.parseObject(responseContent, Meta.class));
					return m_metaCache.get();
				} else if (statusCode == HttpStatus.SC_NOT_MODIFIED) {
					return m_metaCache.get();
				}

			} catch (Exception e) {
				log.debug("Load meta failed, from URL " + url, e);
				// ignore
			}
		}
		throw new RuntimeException(String.format("Failed to load remote meta from %s", ipPorts));
	}

}
