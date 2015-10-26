package com.ctrip.hermes.metaservice.monitor.event;

import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public abstract class AbstractMonitorEvent {
	public static AbstractMonitorEvent parse(com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDal) {
		MonitorEventType type = MonitorEventType.getFromTypeCode(monitorEventDal.getEventType());
		if (type != null) {
			try {
				AbstractMonitorEvent instance = type.getClazz().newInstance();
				instance.parse0(monitorEventDal);
				return instance;
			} catch (Exception e) {
				throw new IllegalArgumentException(String.format("Can not monitor event instance for type[%s]", type), e);
			}
		}
		return null;
	}

	public abstract MonitorEventType getType();

	public abstract com.ctrip.hermes.metaservice.model.MonitorEvent toMonitorEventDal();

	public abstract MonitorEvent parse0(com.ctrip.hermes.metaservice.model.MonitorEvent monitorEventDal);
}
