package com.ctrip.hermes.monitor.service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ctrip.hermes.admin.core.service.EndpointService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.domain.MonitorItem;

@Service
public class ESMonitorService {

	private static final SimpleDateFormat INDEX_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");

	public static final String MONITOR_INDEX_PREFIX = "monitor-";

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");

	static {
		JSON.DEFFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	}

	private TransportClient client;

	@SuppressWarnings("unused")
	private EndpointService endpointService;

	@Autowired
	private MonitorConfig config;

	@PostConstruct
	private void postConstruct() {
		if (config.isMonitorCheckerEnable()) {
			endpointService = PlexusComponentLocator.lookup(EndpointService.class);
			Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", config.getEsClusterName()).build();
			client = new TransportClient(settings);
			String[] esTransportAddress = config.getEsTransportAddress();
			for (int i = 0; i < esTransportAddress.length; i++) {
				String[] split = esTransportAddress[i].split(":");
				client.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
			}
		}
	}

	public IndexResponse prepareIndex(MonitorItem item) throws IOException {
		IndexRequestBuilder builder = client.prepareIndex(MONITOR_INDEX_PREFIX + INDEX_DATE_FORMAT.format(new Date()), item.getCategory(), generateId(item));
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
		if (client != null) {
			client.close();
		}
	}

	private String generateId(MonitorItem item) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.getHost()).append('_').append(item.getSource()).append('_').append(item.getGroup()).append('_').append(item.getCategory()).append('_')
		      .append(formatter.format(item.getStartDate()));
		return sb.toString();
	}

	// *********************** FOR ES QUERY *********************** //

	public MonitorItem queryLatestMonitorItem(String category) {
		SearchResponse response = client.prepareSearch(MONITOR_INDEX_PREFIX + INDEX_DATE_FORMAT.format(new Date())).setTypes(category).setSize(1)
		      .addSort("startDate", SortOrder.DESC).execute().actionGet();
		MonitorItem latestItem = null;
		SearchHits hits = response.getHits();
		if (hits.getTotalHits() > 0) {
			String result = hits.getHits()[0].getSourceAsString();
			latestItem = JSON.parseObject(result, MonitorItem.class);
		}
		return latestItem;
	}
}
