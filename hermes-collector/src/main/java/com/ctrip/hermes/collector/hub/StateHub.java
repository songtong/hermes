package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.exception.NoticeException;
import com.ctrip.hermes.collector.notice.Noticeable;
import com.ctrip.hermes.collector.rule.RuleEventHandlerRegistry;
import com.ctrip.hermes.collector.rule.RulesEngine;
import com.ctrip.hermes.collector.state.HeadState;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;

@Component
public class StateHub {
	private static final Logger LOGGER = LoggerFactory.getLogger(StateHub.class);
	private ConcurrentHashMap<Class<?>, BlockingQueue<State>> m_queues = new ConcurrentHashMap<>();
	private Set<Class<?>> m_runnings = new HashSet<>();
	private Map<Class<? extends State>, HeadState> m_states = new HashMap<>();

	@Autowired
	private StorageManager m_storageManager;

	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private RulesEngine m_rulesEngine;

	@Autowired
	private NotifierManager m_notifierManager;
	
	@Autowired
	private RuleEventHandlerRegistry m_ruleEventRegistry;

	public void offer(State state) {
		if (state == null) {
			LOGGER.warn("Found one NULL state.");
			return;
		}
		
		try {
			m_queues.putIfAbsent(state.getClass(), new ArrayBlockingQueue<State>(m_conf.getStatehubQueueSize()));
			m_queues.get(state.getClass()).put(state);
		} catch (InterruptedException e) {
			// ignore.
		}
		
		if (!m_runnings.contains(state.getClass())) {
			synchronized(this) {
				m_runnings.add(state.getClass());
				Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(StateHub.class)).submit(new StateUpdateTask(m_queues.get(state.getClass())));
			}
		}
		
		m_storageManager.offer(state);
		
		// Send the state to the notifier manager if it's a valid state.
		if (state instanceof Noticeable) {
			try {
				m_notifierManager.offer(((Noticeable)state).get(), null);
			} catch (NoticeException e) {
				LOGGER.error("Failed to send notice for noticeable state: {}", state);
			}
		}
	}

	public class StateUpdateTask implements Runnable {
		private BlockingQueue<State> m_queue;
		
		public StateUpdateTask(BlockingQueue<State> m_queue) {
			this.m_queue = m_queue;
		}
		
		public void handleUpdate(State state) {
			HeadState head = null;
			state.addObserver(m_rulesEngine);
			if ((head = m_states.get(state.getClass())) == null) {
				m_states.put(state.getClass(), head = new HeadState());
			}
			head.addState(state);
		}
		
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				State state = null;
				List<State> states = new ArrayList<State>();
				try {
					if ((state = m_queue.take()) != null) {
						handleUpdate(state);
					}
					
					while (m_queue.drainTo(states) > 0) {
						for (State s : states) {
							handleUpdate(s);
						}
						states.clear();
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
