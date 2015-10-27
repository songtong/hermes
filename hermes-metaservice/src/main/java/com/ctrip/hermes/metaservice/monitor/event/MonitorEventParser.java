package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MonitorEventParser {
	public static MonitorEvent parse(com.ctrip.hermes.metaservice.model.MonitorEvent originEvent) {
		MonitorEventType type = MonitorEventType.getFromTypeCode(originEvent.getEventType());
		if (type != null) {
			try {
				MonitorEvent instance = type.getClazz().newInstance();
				instance.parse(originEvent);
				return (MonitorEvent) instance;
			} catch (Exception e) {
				throw new IllegalArgumentException(String.format("Construct monitor event instance failed [%s]", type), e);
			}
		}
		return null;
	}
}
