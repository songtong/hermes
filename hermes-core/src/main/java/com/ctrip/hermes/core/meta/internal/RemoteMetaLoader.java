package com.ctrip.hermes.core.meta.internal;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.remote.MetaServerLocator;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.io.ByteStreams;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	public static final String ID = "remote-meta-loader";

	@Inject
	private ClientEnvironment m_clientEnvironment;

	@Inject
	private MetaServerLocator m_metaServerLocator;

	private Meta m_meta;

	@Override
	public Meta load() {
		// TODO
		String ipPort = null;
		while (true) {
			// TODO
			System.out.println("try get metaserver ip port list");
			List<String> ipPorts = m_metaServerLocator.getMetaServerIpPorts();
			if (CollectionUtil.isNotEmpty(ipPorts)) {
				ipPort = ipPorts.get(0);
				break;
			} else {
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					return null;
				}
			}
		}
		System.out.println("Loading meta from server: " + ipPort);

		try {
			String url;
			if (m_meta != null) {
				url = "http://" + ipPort + "/meta?hashCode=" + m_meta.hashCode();
			} else {
				url = "http://" + ipPort + "/meta";
			}
			URL metaURL = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) metaURL.openConnection();
			// TODO
			connection.setConnectTimeout(2000);
			connection.setReadTimeout(2000);
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
			throw new RuntimeException("Load remote meta failed", e);
		}
		return m_meta;
	}

}
