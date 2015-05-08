package com.ctrip.hermes.core.meta.remote;

import java.util.List;
import java.util.function.Function;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
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
import com.ctrip.hermes.core.meta.MetaProxy;

@Named(type = MetaProxy.class, value = RemoteMetaProxy.ID)
public class RemoteMetaProxy implements MetaProxy, Initializable {

	public final static String ID = "remote";

	@Inject
	private MetaServerLocator m_metaServerLocator;

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig;

	@Override
	public Lease tryAcquireConsumerLease(Tpg tpg) {
		String response = post("/lease/acquire", tpg);
		if (response != null) {
			return JSON.parseObject(response, Lease.class);
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

	private String post(final String path, final Object payload) {
		return pollMetaServer(new Function<String, String>() {

			@Override
			public String apply(String ip) {
				String url = String.format("http://%s%s", ip, path);
				HttpPost post = new HttpPost(url);
				post.setConfig(m_requestConfig);

				HttpResponse response;
				try {
					post.setEntity(new StringEntity(JSON.toJSONString(payload), ContentType.APPLICATION_JSON));
					response = m_httpClient.execute(post);
					if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
						return EntityUtils.toString(response.getEntity());
					} else {
						// TODO log
						System.out.println("POST ERROR " + url);
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
