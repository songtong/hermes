package com.ctrip.hermes.core.meta.internal;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.remote.MetaServerLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.io.ByteStreams;

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

	private Meta m_meta;

	@Override
	public Meta load() {
		String ipPort = null;
		List<String> ipPorts = m_metaServerLocator.getMetaServerList();
		if (ipPorts == null || ipPorts.isEmpty()) {
			throw new RuntimeException("No meta server found.");
		}
		ipPort = ipPorts.get(0);
		log.info("Loading meta from server: {}", ipPort);

		try {
			String url;
			if (m_meta != null) {
				url = "http://" + ipPort + "/meta?hashCode=" + m_meta.hashCode();
			} else {
				url = "http://" + ipPort + "/meta";
			}
			URL metaURL = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) metaURL.openConnection();
			connection.setConnectTimeout(m_config.getMetaServerConnectTimeout());
			connection.setReadTimeout(m_config.getMetaServerReadTimeout());
			connection.setRequestMethod("GET");
			connection.connect();
			if (connection.getResponseCode() == 200) {
				InputStream is = connection.getInputStream();
				String jsonString = new String(ByteStreams.toByteArray(is));
				m_meta = JSON.parseObject(jsonString, Meta.class);
			} else if (connection.getResponseCode() == 304) {
				return m_meta;
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to load remote meta", e);
		}
		return m_meta;
	}

}
