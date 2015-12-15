package com.ctrip.hermes.metaservice.monitor;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.CheckerExceptionEvent;
import com.ctrip.hermes.metaservice.monitor.event.ConsumeDelayTooLargeEvent;
import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.monitor.event.LongTimeNoProduceEvent;
import com.ctrip.hermes.metaservice.monitor.event.MetaServerErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.event.PartitionInformationEvent;
import com.ctrip.hermes.metaservice.monitor.event.PartitionModificationEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceAckedTriedRatioErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceFailureCountTooLargeEvent;
import com.ctrip.hermes.metaservice.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.metaservice.monitor.event.TopicLargeDeadLetterEvent;

public enum MonitorEventType {
	BROKER_ERROR(1, "broker_error", BrokerErrorEvent.class), //
	METASERVER_ERROR(2, "metaserver_error", MetaServerErrorEvent.class), //

	PRODUCE_LARGE_LATENCY(3, "produce_large_latency", ProduceLatencyTooLargeEvent.class), //
	PRODUCE_LARGE_FAILURE_COUNT(4, "produce_large_failure_count", ProduceFailureCountTooLargeEvent.class), //
	PRODUCE_ACKED_TRIED_RATIO_ERROR(5, "produce_acked_tried_ratio_error", ProduceAckedTriedRatioErrorEvent.class), //

	CONSUME_LARGE_DELAY(6, "consume_large_delay", ConsumeDelayTooLargeEvent.class), //
	CONSUME_LARGE_BACKLOG(7, "consume_large_backlog", ConsumeLargeBacklogEvent.class), //

	TOPIC_LARGE_DEAD_LETTER(8, "topic_large_dead_letter", TopicLargeDeadLetterEvent.class), //

	PARTITION_MODIFICATION(9, "partition_modification", PartitionModificationEvent.class), //

	LONG_TIME_NO_PRODUCE(10, "long_time_no_produce", LongTimeNoProduceEvent.class), //

	PARTITION_INFO(11, "partition_informations", PartitionInformationEvent.class), //

	ES_DATASOURCE_ERROR(40, null, null), //
	CAT_DATASOURCE_ERROR(41, null, null), //
	DB_DATASOURCE_ERROR(42, null, null), //

	CHECKER_EXCEPTION(50, "checker_exception", CheckerExceptionEvent.class), //
	;

	private int m_code;

	private String m_displayName;

	private Class<? extends MonitorEvent> m_clazz;

	private MonitorEventType(int code, String displayName, Class<? extends MonitorEvent> clazz) {
		m_code = code;
		m_displayName = displayName;
		m_clazz = clazz;
	}

	public int getCode() {
		return m_code;
	}

	public String getDisplayName() {
		return StringUtils.isBlank(m_displayName) ? name() : m_displayName;
	}

	public Class<? extends MonitorEvent> getClazz() {
		return m_clazz;
	}

	public static MonitorEventType findByTypeCode(int code) {
		for (MonitorEventType type : MonitorEventType.values()) {
			if (type.getCode() == code) {
				return type;
			}
		}
		return null;
	}
}
