package com.ctrip.hermes.monitor.service;

import java.io.IOException;
import java.text.SimpleDateFormat;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.domain.MonitorItem;

@Service
public class ESMonitorService {

	public static final String DEFAULT_INDEX = "monitor";

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
	static {
		JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	}

	private TransportClient client;

	@Autowired
	private MonitorConfig config;

	@PostConstruct
	private void postConstruct() {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getEsClusterName()).build();
		client = new TransportClient(settings).addTransportAddresses();
		String[] esTransportAddress = config.getEsTransportAddress();
		for (int i = 0; i < esTransportAddress.length; i++) {
			String[] split = esTransportAddress[i].split(":");
			client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
		}
	}

	public IndexResponse prepareIndex(MonitorItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(DEFAULT_INDEX, item.getCategory(), generateId(item));
		String source = JSON.toJSONString(item, SerializerFeature.WriteDateUseDateFormat);
		IndexResponse response = builder.setSource(source).execute().actionGet();
		return response;
	}

	public GetResponse prepareGet(String index, String type, String id) {
		GetResponse response = client.prepareGet(index, type, id).execute().actionGet();
		return response;
	}

	@PreDestroy
	private void preDestory() {
		client.close();
	}

	private String generateId(MonitorItem item) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.getHost()).append('_').append(item.getSource()).append('_').append(item.getCategory()).append('_')
		      .append(formatter.format(item.getStartDate()));
		return sb.toString();
	}
}
