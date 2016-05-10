package com.ctrip.hermes.collector.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.pipe.Pipe;
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.pipe.PipelineRegistry;
import com.ctrip.hermes.collector.state.State;

@Component
public class RulesEngine extends Pipe<State> {
	private static Logger logger = LoggerFactory.getLogger(RulesEngine.class);
	private AtomicBoolean m_running = new AtomicBoolean(false);
	private BlockingQueue<State> m_queue;
	private ExecutorService m_executors;
	@Autowired
	private PipelineRegistry m_registry;
	
	public RulesEngine() {
		super(State.class);
	}
	
	@PostConstruct
	private void init() {
		m_queue = new ArrayBlockingQueue<State>(1000);
		m_executors = Executors.newSingleThreadExecutor();
		m_executors.submit(new RulesApplyTask());
		m_running.compareAndSet(false, true);
	}

	@Override
	public boolean validate(State state) {
		return state != null;
	}

	@Override
	public void doProcess(PipeContext context, State state) throws Exception {
		if (!m_queue.offer(state)) {
			logger.error("Can NOT put state into rules queue!");
		}
		// Abort the processing on pipeline.
		context.abort();
	}
	
	public class RulesApplyTask implements Runnable {
		@Override
		public void run() {
			try {
				List<State> states = new ArrayList<State>();
				while (!Thread.interrupted()) {
					int size = m_queue.drainTo(states);
					if (size > 0) {
						for (int index = 0; index < size; index++) {
							// Continue the pipeline processing.
							RulesEngine.this.processNext(new PipeContext(), states.get(index));
						}
					}
				}
			} finally {
				m_running.compareAndSet(true, false);
			}
		}
	}
	
}
