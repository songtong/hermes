package com.ctrip.hermes.collector.rule.eventhandler;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.elasticsearch.common.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.EPLs;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.KVService;
import com.ctrip.hermes.metaservice.service.KVService.Tag;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;
import com.google.common.base.Charsets;

@Component
@TriggeredBy({ TPGConsumeState.class })
@EPLs({ @EPL(name = ConsumerBillCheckEventHandler.EVENT_ARRIVED, value = ConsumerBillCheckEventHandler.ESPER_EXPRESSION) })
public class ConsumerBillCheckEventHandler extends RuleEventHandler {
	private static final Logger log = LoggerFactory.getLogger(ConsumerBillCheckEventHandler.class);

	public static final String EVENT_ARRIVED = "BILL_EVENT_ARRIVED";

	public static final String ESPER_EXPRESSION = "select * from com.ctrip.hermes.collector.state.impl.TPGConsumeState(not sync)";

	public static final String MAPPING_FILE_NAME = "cb_mapping.properties";

	public static final String DEFAULT_BU_NAME = "UNKNOWN";

	private static final int DEFAULT_PERSIST_INTERVAL_MINUTES = 5;

	private ScheduledExecutorService m_executor = Executors.newSingleThreadScheduledExecutor();

	private ConcurrentHashMap<Tpg, Long> m_consumeOffsets = new ConcurrentHashMap<>();

	private KVService m_kvService = PlexusComponentLocator.lookup(KVService.class);

	private Properties m_consumerBuMapping = new Properties();

	@PostConstruct
	public void init() {
		super.init();

		loadConsumerBuMapping();

		m_consumeOffsets.putAll(loadOldOffsets());

		m_executor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				for (Entry<Tpg, Long> entry : m_consumeOffsets.entrySet()) {
					Tpg tpg = entry.getKey();
					String kvKey = String.format("%s#%s#%s", tpg.getTopic(), tpg.getPartition(), tpg.getGroupId());
					String kvValue = String.valueOf(entry.getValue());
					try {
						m_kvService.setKeyValue(kvKey, kvValue, Tag.BILL);
					} catch (Exception e) {
						log.warn("Set kv failed:({}, {})", kvKey, kvValue);
					}
				}
			}
		}, DEFAULT_PERSIST_INTERVAL_MINUTES, DEFAULT_PERSIST_INTERVAL_MINUTES, TimeUnit.MINUTES);
	}

	private Map<Tpg, Long> loadOldOffsets() {
		Map<String, String> dbEntities = m_kvService.list(Tag.BILL);
		Map<Tpg, Long> old = new HashMap<>();
		for (Entry<String, String> entry : dbEntities.entrySet()) {
			String[] parts = entry.getKey().split("#");
			if (3 == parts.length) {
				Tpg tpg = new Tpg(parts[0], Integer.valueOf(parts[1]), parts[2]);
				Long oldConsumeOffset = Long.valueOf(entry.getValue());
				old.put(tpg, oldConsumeOffset);
			}
		}
		return old;
	}

	private void loadConsumerBuMapping() {
		try {
			m_consumerBuMapping.load(new InputStreamReader(getClass().getClassLoader().getResourceAsStream(MAPPING_FILE_NAME), Charsets.UTF_8));
		} catch (IOException e) {
			throw new RuntimeException("Can not load consumer-bu mapping config.");
		}
	}

	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		if (event.getEventSource().equals(EVENT_ARRIVED)) {
			@SuppressWarnings("unchecked")
			EventBean[] newEvents = ((Pair<EventBean[], EventBean[]>) event.getData()).getFirst();
			if (newEvents != null) {
				for (EventBean bean : newEvents) {
					Tpg tpg = new Tpg(String.valueOf((long) bean.get("topicId")), (int) bean.get("partition"), String.valueOf((long) bean
					      .get("consumerGroupId")));
					String consumerName = (String) bean.get("consumerGroup");
					String storageType = (String) bean.get("storageType");

					if (!StringUtils.isBlank(consumerName)) {
						long pOffset = bean.get("offsetPriority") != null ? (long) bean.get("offsetPriority") : 0;
						long npOffset = bean.get("offsetNonPriority") != null ? (long) bean.get("offsetNonPriority") : 0;
						long totalOffset = pOffset + npOffset;

						Long old = m_consumeOffsets.get(tpg);
						if (old != null) {
							totalOffset = Math.max(old, totalOffset);
							checkBill(consumerName, totalOffset, old, storageType);
						}
						m_consumeOffsets.put(tpg, totalOffset);
					} else {
						log.warn("Consumer name is blank: {}", bean);
					}
				}
			}
		}
		return null;
	}

	private void checkBill(String consumerName, long current, long old, String storageType) {
		long delta = Math.max(0, current - old);
		String bu = getBuFromConsumerName(consumerName);

		if (delta > 0 && !StringUtils.isBlank(bu)) {
			if (DEFAULT_BU_NAME.equals(bu)) {
				Transaction t = Cat.newTransaction(CatConstants.TYPE_HERMES_BILL_UNKNOWN, consumerName);
				t.addData("*count", delta);
				t.setStatus(Message.SUCCESS);
				t.complete();
			}

			String catName = Storage.KAFKA.equals(storageType) ? CatConstants.NAME_HERMES_BILL_KAFKA : CatConstants.NAME_HERMES_BILL_MYSQL;
			Transaction t = Cat.newTransaction(CatConstants.TYPE_HERMES_BILL, catName);
			t.addData("*count", delta);
			t.addData("*bu", bu);
			t.setStatus(Message.SUCCESS);
			t.complete();
		}
	}

	private String getBuFromConsumerName(String consumer) {
		String[] parts = consumer.split("\\.");
		String bu = m_consumerBuMapping.getProperty(parts[0]);

		if ("none".equalsIgnoreCase(bu)) {
			return null;
		}

		if (StringUtils.isBlank(bu) && parts.length > 1) {
			bu = m_consumerBuMapping.getProperty(parts[1]);
		}

		return StringUtils.isBlank(bu) ? DEFAULT_BU_NAME : bu;
	}

	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		// TODO Auto-generated method stub
		return null;
	}

}
