package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.CollectorRegistry;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.producer.api.Producer;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;

@Component
public class StorageManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(StorageManager.class);
	private BlockingQueue<State> m_queue;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private CollectorRegistry m_registry;
	
	private Producer m_producer;
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<State>(m_conf.getStorageQueueSize());
		Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(DataHub.class.getSimpleName())).submit(new StorageTask());
		m_producer = Producer.getInstance();
	}
	
	public void offer(State state) {
		if (!m_queue.offer(state)) {
			LOGGER.error("Failed to save state into storage queue: {}", state);
		}
	}
	
	public class StorageTask implements Runnable {
		
		@Override
		public void run() {
			List<State> states = new ArrayList<State>();
			State state = null;
			while (!Thread.interrupted()) {
				try {
					if ((state = m_queue.take()) != null) {
						m_producer.message(m_conf.getStorageTopic(), state.getId().toString(), state.toString()).send();
						sampleState(state);
					}
					
					while (m_queue.drainTo(states) > 0) {
						for (State s : states) {
							m_producer.message(m_conf.getStorageTopic(), s.getId().toString(), s.toString()).send();
						}
						LOGGER.info("Saved batch states with size: {}", states.size());
						states.clear();
					}
				} catch (Exception e) {
					LOGGER.error("Exception occurs while saving states: ", e);
				}
			}
		}
		
		// Sample state & log to cat.
		private void sampleState(State state) {
			Event event = Cat.newEvent("State", "Sampling");
			event.addData("content", state);
			event.setStatus(Message.SUCCESS);
			event.complete();
			LOGGER.info("Sampling: {}", state);
		}
	} 
}
