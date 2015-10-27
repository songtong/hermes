package com.ctrip.hermes.metaservice.monitor;

import com.ctrip.hermes.metaservice.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MetaServerErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;

public enum MonitorEventType {
	BROKER_ERROR(1, BrokerErrorEvent.class), //
	METASERVER_ERROR(2, MetaServerErrorEvent.class), //

	PRODUCE_LONG_LATENCY(3, null), //
	PRODUCE_HIGH_FAILURE_RATIO(4, null), //
	PRODUCE_HIGH_RESEND_RATIO(5, null), //

	CONSUME_LONG_DELAY(6, null), //
	CONSUME_LARGE_BACKLOG(7, null), //

	ES_DATASOURCE_ERROR(8, null), //
	CAT_DATASOURCE_ERROR(9, null)//
	;

	private int m_code;

	private Class<? extends MonitorEvent> m_clazz;

	private MonitorEventType(int code, Class<? extends MonitorEvent> clazz) {
		m_code = code;
		m_clazz = clazz;
	}

	public int getCode() {
		return m_code;
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
