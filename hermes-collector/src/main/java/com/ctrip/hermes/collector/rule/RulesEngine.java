package com.ctrip.hermes.collector.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.hub.SuspendPolicy;
import com.ctrip.hermes.collector.pipe.Pipe;
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.pipe.Pipes;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.espertech.esper.client.Configuration;

@Component
public class RulesEngine extends Pipe<State> implements Observer {
	private static final Logger LOGGER = LoggerFactory.getLogger(RulesEngine.class);
	private Map<Class<?>, BlockingQueue<State>> m_queues = new HashMap<>();
	private Map<Class<?>, AtomicBoolean> m_runnings = new HashMap<>();
	private ExecutorService m_executors;
	private ExecutorService m_distributionExecutors;
	private Pipes<State> m_ruleEventPipes = new Pipes<>(State.class);
	
	@Autowired
	private RuleEventHandlerRegistry m_ruleEventHandlerRegistry;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	public RulesEngine() {
		super(State.class);
	}

	@PostConstruct
	private void init() {
		if (m_conf.isEnableRealtimeAlert()) {
			for (Class<?> handledType : m_ruleEventHandlerRegistry.getRegisteredTypes()) {
				m_queues.put(handledType, new ArrayBlockingQueue<State>(m_conf.getRuleEngineQueueSize()));
				m_runnings.put(handledType, new AtomicBoolean(false));
			}
			
			m_distributionExecutors = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(RulesEngine.class.getSimpleName()));
			m_executors = Executors.newCachedThreadPool();
			// Start distribution task.
			m_distributionExecutors.submit(new EventDistributionTask());
			initEsper();
		}
	}
	
	private void initEsper() {
		Configuration conf = new Configuration();
		//conf.configure();
		
		// Create rule event pipe according to event handlers.
		for (RuleEventHandler eventHandler : m_ruleEventHandlerRegistry.getRegisteredHandlers()) {
			EsperEventPipe<State> ruleEventPipe = new EsperEventPipe<>(State.class, eventHandler);
			ruleEventPipe.setup(conf);
			m_ruleEventPipes.addPipe(ruleEventPipe);
		}
	}
	
	@Override
	public void update(Observable o, Object arg) {
		if (m_conf.isEnableRealtimeAlert()) {
			State state = (State)arg;
			if (isExpired(state)) {
				return;
			}
			
			BlockingQueue<State> queue = m_queues.get(state.getClass());
			if (queue == null) {
				LOGGER.error("Found event it's unknown: {}", state.getClass());
				return;
			}
			
			if (!queue.offer(state)) {
				if (state.isSync()) {
					try {
						queue.put(state);
					} catch (InterruptedException e) {
						// Ignore.
					}
				} else {
					LOGGER.error("Can NOT put state {} into rules engine queue.", state);
				}
			}
		}
	}
	
	private boolean isExpired(State state) {
		return System.currentTimeMillis() - state.getTimestamp() > TimeUnit.MINUTES.toMillis(10); 
	}

	// Allow to handle all kinds of states.
	@Override
	public boolean validate(State state) {
		return state != null;
	}

	@Override
	public void doProcess(PipeContext context, State state) throws Exception {
		// Apply rules on each of the pipes.
		context.setResetEveryPipe(true);
		m_ruleEventPipes.process(context, state);
	}
	
	// Task for distribution of events.
	public class EventDistributionTask implements Runnable {
	
		@Override
		public void run() {
			SuspendPolicy policy = SuspendPolicy.instance();
			while (!Thread.interrupted()) {
				boolean succeed = false;
				for (Map.Entry<Class<?>, BlockingQueue<State>> entry : m_queues.entrySet()) {
					if (entry.getValue().size() > 0) {
						if (m_runnings.get(entry.getKey()).compareAndSet(false, true)) {
							List<State> states = new ArrayList<State>();
							entry.getValue().drainTo(states);
							m_executors.submit(new RulesApplyTask(states));
							succeed = true;
						}
					}
				}
				policy.suspend(succeed);
			}
		}
		
	}

	// Task for applying rules.
	public class RulesApplyTask implements Runnable {
		private List<State> m_states;
		
		public RulesApplyTask(List<State> events) {
			this.m_states = events;
		}
		
		@Override
		public void run() {
			for (State state : m_states) {
				process(new PipeContext(), state);
			}
			m_runnings.get(m_states.get(0).getClass()).compareAndSet(true, false);
		}
	}
}
