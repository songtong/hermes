package com.ctrip.hermes.utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.Tpg;

public class RestTest {

	private HttpClient m_httpClient;

	private RequestConfig m_requestConfig = RequestConfig.DEFAULT;

	@Test
	public void test() throws Exception {
		m_httpClient = HttpClients.createDefault();

		Builder b = RequestConfig.custom();
		// TODO config
		b.setConnectTimeout(2000);
		b.setSocketTimeout(2000);
		m_requestConfig = b.build();

		String url = String.format("http://%s%s", "127.0.0.1:1248", "/lease/acquire");
		HttpPost post = new HttpPost(url);
		post.setConfig(m_requestConfig);

		HttpResponse response;
		post.setEntity(new StringEntity(JSON.toJSONString(new Tpg("topic", 1, "1")), ContentType.APPLICATION_JSON));
		response = m_httpClient.execute(post);
		System.out.println(EntityUtils.toString(response.getEntity()));
	}

}
