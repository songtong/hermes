package com.ctrip.hermes.core.meta.internal;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.lookup.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.io.ByteStreams;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	public static final String ID = "remote-meta-loader";

	@Inject
	private ClientEnvironment m_clientEnvironment;

	private Meta m_meta;

	private String host;

	private String port;

	@Override
	public Meta load() {
		try {
			if (StringUtils.isEmpty(host)) {
				System.out.println("Loading meta from server: " + host);
				host = m_clientEnvironment.getGlobalConfig().getProperty("meta-host");
			}
			if (StringUtils.isEmpty(port)) {
				port = m_clientEnvironment.getGlobalConfig().getProperty("meta-port");
			}
			String url;
			if (m_meta != null) {
				url = "http://" + host + ":" + port + "/meta?hashCode=" + m_meta.hashCode();
			} else {
				url = "http://" + host + ":" + port + "/meta";
			}
			URL metaURL = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) metaURL.openConnection();
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

	@Override
	public boolean save(Meta meta) {
		try {
			if (StringUtils.isEmpty(host)) {
				System.out.println("Saving mete to server: " + host);
				host = m_clientEnvironment.getGlobalConfig().getProperty("meta-host");
			}
			if (StringUtils.isEmpty(port)) {
				port = m_clientEnvironment.getGlobalConfig().getProperty("meta-port");
			}
			String url = "http://" + host + ":" + port + "/meta";
			URL metaURL = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) metaURL.openConnection();
			conn.setRequestMethod("POST");
			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			String jsonString = JSON.toJSONString(meta);
			writer.write(jsonString);
			writer.close();
		} catch (Exception e) {
			throw new RuntimeException("Save remote meta failed", e);
		}
		return true;
	}
}
