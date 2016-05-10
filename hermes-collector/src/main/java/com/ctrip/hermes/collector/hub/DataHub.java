package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

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
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;

//@Component
public class DataHub extends RecordPipe {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataHub.class);
	private BlockingQueue<Record<?>> m_queue;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@Autowired
	private PipelineRegistry m_registry;
	
	@PostConstruct
	protected void init() {
		m_queue = new ArrayBlockingQueue<Record<?>>(m_conf.getDatahubQueueSize());
		Executors.newSingleThreadExecutor(CollectorThreadFactory.newFactory(DataHub.class.getSimpleName())).submit(new PipelineProcessTask());
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
			LOGGER.warn("Ignore one NULL value record!");
			return;
		}

		if (!m_queue.offer(record)) {
			LOGGER.error("Failed to put record into datahub queue: {}", record);
		}
	}
	
	public class PipelineProcessTask implements Runnable {
		private List<Record<?>> m_records = new ArrayList<Record<?>>();
		
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				try {
					// Block for records incoming in the queue.
					Record<?> record = m_queue.take();
					
					// If still has records in the queue, drain to the list.
					if (m_queue.size() > 0) {
						m_queue.drainTo(m_records);
					} 
					
					// Add taken record back into the drained list.
					m_records.add(record);
					for (int index = 0; index < m_records.size(); index++) {
						DataHub.this.process(new PipeContext(), m_records.get(index));
					}
					LOGGER.info("Done handling batch records: {}", m_records.size());
					m_records.clear();
				} catch (Exception e) {
					e.printStackTrace();
					LOGGER.info("DataHub pipeline prorcess error: " + e);
				}
			}
		}		
	}
}
