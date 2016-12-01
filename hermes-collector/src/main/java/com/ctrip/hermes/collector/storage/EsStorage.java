package com.ctrip.hermes.collector.storage;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.producer.api.Producer;

/**
 * @author tenglinxiao
 *
 */
@Component(EsStorage.ES_STORAGE)
public class EsStorage extends Storage {
	public static final String ES_STORAGE = "es";
	
	private Producer m_producer;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	@PostConstruct
	public void init() {
		m_producer = Producer.getInstance();
	}
	
	public boolean save(State state) {
		m_producer.message(m_conf.getStorageTopic(), state.getId().toString(), state).send();
		return true;
	}

}
