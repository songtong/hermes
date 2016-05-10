package com.ctrip.hermes.collector.hub;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.pipe.ConversionPipe;
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.States;

@Component
public class StateHub extends ConversionPipe<Record<?>, State> implements ApplicationContextAware {
	private static Logger LOGGER = LoggerFactory.getLogger(StateHub.class);
	private ApplicationContext m_applicationContext;
	private Map<Class<?>, State> m_states = new ConcurrentHashMap<Class<?>, State>();
	private ExecutorService m_executors;
	private ArrayBlockingQueue<State> m_queue;
	
	@Autowired
	private StorageManager m_storageManager;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	public StateHub() {
		super(Record.class);
	}
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<State>(m_conf.getStatehubQueueSize());
		m_executors = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(StateHub.class.getSimpleName()));
		// Set next pipe be rules engine.
		setHubNext(m_applicationContext.getBean(RulesEngine.class));
		m_executors.submit(new StateUpdateTask());
	}

	@Override
	public boolean validate(Record<?> record) {
		return record != null;
	}

	@Override
	public void doProcess(PipeContext context,
			Record<?> record) throws Exception {
		// Flatten states to state & put into the queue for updating.
		if (context.getState() instanceof States) {
			for (State state : ((States)context.getState()).getStates().values()) {
				offer(state);
			}
		} else {
			offer(context.getState());
		}
	}
		
	private void offer(State state) {
		if (!m_queue.offer(state)) {
			LOGGER.error("Failed to put state into queue of statehub!");
		}
		
		m_storageManager.offer(state);
	}
	
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.m_applicationContext = applicationContext;
	}
	
	public class StateUpdateTask implements Runnable {
		@Override
		public void run() {	
			while (!Thread.interrupted()) {
				State state = null;
				State existedState = null;
				try {
					if ((state = m_queue.take()) != null) {
						if ((existedState = m_states.get(state.getClass())) == null) {
							m_states.put(state.getClass(), state);
						} else {
							existedState.update(state);
						}
					}
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
	
}
