package com.ctrip.hermes.admin.core.monitor;

import com.ctrip.hermes.admin.core.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.CheckerExceptionEvent;
import com.ctrip.hermes.admin.core.monitor.event.ConsumeDelayTooLargeEvent;
import com.ctrip.hermes.admin.core.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.admin.core.monitor.event.ConsumerAckErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoConsumeEvent;
import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoProduceEvent;
import com.ctrip.hermes.admin.core.monitor.event.MetaRequestErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.MetaServerErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.PartitionInformationEvent;
import com.ctrip.hermes.admin.core.monitor.event.PartitionModificationEvent;
import com.ctrip.hermes.admin.core.monitor.event.ProduceFailureCountTooLargeEvent;
import com.ctrip.hermes.admin.core.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.admin.core.monitor.event.ProduceTransportFailedRatioErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.TopicLargeDeadLetterEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.BrokerLeaseErrorMonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.BrokerLeaseTimeoutMonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.ConsumerLeaseErrorMonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.lease.ConsumerLeaseTimeoutMonitorEvent;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.monitor.event.BrokerCommandDropEvent;
import com.ctrip.hermes.metaservice.monitor.event.ConsumerDeadLetterEvent;

public enum MonitorEventType {
	BROKER_ERROR(1, "broker_error", BrokerErrorEvent.class, true), //
	METASERVER_ERROR(2, "metaserver_error", MetaServerErrorEvent.class, true), //

	PRODUCE_LARGE_LATENCY(3, "produce_large_latency", ProduceLatencyTooLargeEvent.class, true), //
	PRODUCE_LARGE_FAILURE_COUNT(4, "produce_large_failure_count", ProduceFailureCountTooLargeEvent.class, true), //

	CONSUME_LARGE_DELAY(6, "consume_large_delay", ConsumeDelayTooLargeEvent.class, true), //
	CONSUME_LARGE_BACKLOG(7, "consume_large_backlog", ConsumeLargeBacklogEvent.class, true), //
	TOPIC_LARGE_DEAD_LETTER(8, "topic_large_dead_letter", TopicLargeDeadLetterEvent.class, true), //
	PARTITION_MODIFICATION(9, "partition_modification", PartitionModificationEvent.class, true), //
	LONG_TIME_NO_PRODUCE(10, "long_time_no_produce", LongTimeNoProduceEvent.class, true), //
	PARTITION_INFO(11, "partition_informations", PartitionInformationEvent.class, false), //
	PRODUCE_TRANSPORT_FAILED_RATIO_ERROR(12, "produce_transport_failed_ratio_error",
	      ProduceTransportFailedRatioErrorEvent.class, true), //
	CONSUMER_LEASE_ERROR(13, "consumer_lease_error", ConsumerLeaseErrorMonitorEvent.class, true), //
	CONSUMER_LEASE_TIMEOUT(14, "consumer_lease_timeout", ConsumerLeaseTimeoutMonitorEvent.class, true), //
	BROKER_LEASE_ERROR(15, "broker_lease_error", BrokerLeaseErrorMonitorEvent.class, true), //
	BROKER_LEASE_TIMEOUT(16, "broker_lease_timeout", BrokerLeaseTimeoutMonitorEvent.class, true), //
	META_REQUEST_ERROR(17, "meta_request_error", MetaRequestErrorEvent.class, true), //
	CONSUMER_ACK_CMD_ERROR(18, "consumer_ack_cmd_error", ConsumerAckErrorEvent.class, true), //
	LONG_TIME_NO_CONSUME(19, "long_time_no_consume", LongTimeNoConsumeEvent.class, true), //
	CONSUMER_DEAD_LETTER(20, "consumer_dead_letter", ConsumerDeadLetterEvent.class, true), //
	BROKER_COMMAND_DROP(21, "broker_command_drop", BrokerCommandDropEvent.class, true), 

	ES_DATASOURCE_ERROR(40, null, null, true), //
	CAT_DATASOURCE_ERROR(41, null, null, true), //
	DB_DATASOURCE_ERROR(42, null, null, true), //

	CHECKER_EXCEPTION(50, "checker_exception", CheckerExceptionEvent.class, true), //
	;

	private int m_code;

	private String m_displayName;

	private Class<? extends MonitorEvent> m_clazz;

	private boolean m_shouldAlarm;

	private MonitorEventType(int code, String displayName, Class<? extends MonitorEvent> clazz, boolean shouldAlarm) {
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

	public boolean shouldAlarm() {
		return m_shouldAlarm;
	}

	public static MonitorEventType findByTypeCode(int code) {
		for (MonitorEventType type : MonitorEventType.values()) {
			if (type.getCode() == code) {
				return type;
			}
		}
		return null;
	}
	
	public static void main(String[] args) {
	   for (MonitorEventType type : MonitorEventType.values()) {
	      System.out.println(type);
      }
   }
}
