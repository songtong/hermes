package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.List;
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
import com.ctrip.hermes.collector.pipe.PipeContext;
import com.ctrip.hermes.collector.pipe.PipelineRegistry;
import com.ctrip.hermes.collector.pipe.Pipes;
import com.ctrip.hermes.collector.pipe.RecordPipe;
import com.ctrip.hermes.collector.record.Record;

@Component
public class DataHub extends RecordPipe {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataHub.class);
	private BlockingQueue<Record<?>> m_queue = null;
	private ExecutorService m_executors;
	private AtomicBoolean m_running = new AtomicBoolean(false);
	
	@Autowired
	private CollectorConfiguration m_conf;
	@Autowired
	private PipelineRegistry m_registry;
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<Record<?>>(m_conf.getDatahubQueueSize());
		m_executors = Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(DataHub.class.getSimpleName()));
	}
	
	// Allow to handle all kinds of not-null record.
	@Override
	public boolean validate(Record<?> record) {
		return record != null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void doProcess(PipeContext context, Record<?> record) {
		for (Pipes pipes : m_registry.findPipesByType(Record.class).values()) {
			pipes.process(context, record);
		}
	}
	
	public void offer(Record<?> record) {
		if (record == null) {
			LOGGER.warn("Ignore one record with value NULL!");
			return;
		}
		
		// If task is not running, submit a task.
		if (m_running.compareAndSet(false, true)) {
			m_executors.submit(new PipelineProcessTask());
		}
			
		if (!m_queue.offer(record)) {
			try {
				if (m_queue.offer(record, m_conf.getRetryWaitTime(), TimeUnit.MILLISECONDS)) {
					LOGGER.error("Failed to put record into datahub queue!");
				}
			} catch (InterruptedException e) {
				// Ignore.
			}
		}
	}
	
	public class PipelineProcessTask implements Runnable {
		private List<Record<?>> m_records = new ArrayList<Record<?>>();
		
		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					int size = m_queue.drainTo(m_records);
					if (size > 0) {
						for (int index = 0; index < size; index++) {
							DataHub.this.process(new PipeContext(), m_records.get(index));
						}
						LOGGER.info("Done handling msg: " + size);
						m_records.clear();
					} 
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.info("DataHub processing task existed due to exception: " + e);
			} finally {
				m_running.compareAndSet(true, false);
			}
		}
	}
}
