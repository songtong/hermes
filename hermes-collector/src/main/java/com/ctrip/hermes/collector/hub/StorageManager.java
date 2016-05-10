package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.CollectorRegistry;
import com.ctrip.hermes.collector.collector.EsHttpCollector.EsHttpCollectorContext;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder.EsQueryContext;
import com.ctrip.hermes.collector.collector.EsQueryContextBuilder.EsQueryType;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.DatasourceManager;
import com.ctrip.hermes.collector.datasource.EsDatasource;
import com.ctrip.hermes.collector.datasource.HttpDatasource.HttpDatasourceType;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.producer.api.Producer;

@Component
public class StorageManager {
	private static Logger logger = LoggerFactory.getLogger(StorageManager.class);
	private BlockingQueue<State> m_queue;
	private ExecutorService m_storageExecutors;
	private ExecutorService m_distributionExecutors;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private DatasourceManager m_manager;
	
	@Autowired
	private CollectorRegistry m_registry;
	
	private Producer m_produer;
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<State>(1000);
		m_storageExecutors = Executors.newFixedThreadPool(3);
		m_distributionExecutors = Executors.newSingleThreadExecutor();
		m_distributionExecutors.submit(new StorageDistributionTask());
	}
	
	public void offer(State state) {
		if (!m_queue.offer(state)) {
			logger.error("Failed to put record into storage queue!");
		}
	}
	
	public class StorageDistributionTask implements Runnable {
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				if (m_queue.size() >= 1) {
					List<State> states = new ArrayList<State>();
					m_queue.drainTo(states, 1);
					m_storageExecutors.submit(new StorageTask(states));
				}
			}
		}
	}
	
	public class StorageTask implements Runnable {
		private List<State> m_states;
		
		public StorageTask(List<State> states) {
			this.m_states = states;
		}
		
		public State findState(Object id) {
			for (State state : m_states) {
				if (state.getId().equals(id)) {
					return state;
				}
			}
			return null;
		}
		
		@Override
		public void run() {
			EsDatasource datasource = (EsDatasource)m_manager.getDefaultDatasource(HttpDatasourceType.ES);
			EsQueryContext queryContext = EsQueryContextBuilder.newBuilder(EsQueryType.CREATE).data(this.m_states).build();
			CollectorContext context = EsHttpCollectorContext.newContext(datasource, "_bulk", queryContext, RecordType.STORAGE_RESULT);

			try {
				Record<JsonNode> result = m_registry.find(context).collect(context);
				StorageResult storageResult = StorageResult.newResult(result.getData());
				if (storageResult.hasError()) {
					for (String id : storageResult.getItems()) {
						offer(findState(id));
						logger.warn("Reput the states into queue for storage: " + id);
					}
				} else {
					logger.info(String.format("Done saving [%s] states into es!", m_states.size()));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	} 
	
	public static class StorageResult {
		private boolean m_hasError;
		private List<String> m_items;
		
		private StorageResult() {}
		
		public boolean hasError() {
			return m_hasError;
		}

		public void setHasError(boolean hasError) {
			m_hasError = hasError;
		}

		public List<String> getItems() {
			return m_items;
		}

		public void setItems(List<String> items) {
			m_items = items;
		}

		private void parse(JsonNode result) {
			if (result.has("errors")) {
				m_hasError = result.get("errors").asBoolean();
				if (m_hasError) {
					m_items = new ArrayList<String>();
					ArrayNode items = (ArrayNode)result.get("items");
					for (JsonNode item : items) {
						if (item.get("status").asInt() != 201) {
							m_items.add(item.get("_id").asText());
						}
					}
				}
			}
		}
		public static StorageResult newResult(JsonNode result) {
			StorageResult storageResult = new StorageResult();
			storageResult.parse(result);
			return storageResult;
		}
	}
}
