package com.ctrip.hermes.metaservice.monitor.event;

import java.util.List;

import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;

public class MonitorEventParser {
	public static MonitorEvent parse(com.ctrip.hermes.metaservice.model.MonitorEvent dbEntity) {
		MonitorEventType type = MonitorEventType.findByTypeCode(dbEntity.getEventType());
		if (type != null) {
			try {
				MonitorEvent instance = type.getClazz().newInstance();
				instance.parse(dbEntity);
				return (MonitorEvent) instance;
			} catch (Exception e) {
				throw new IllegalArgumentException(String.format("Construct monitor event instance failed [%s]", type), e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static List<MonitorEvent> parse(List<com.ctrip.hermes.metaservice.model.MonitorEvent> dbEntities) {
		return (List<MonitorEvent>) CollectionUtil.collect(dbEntities, new Transformer() {
			@Override
			public Object transform(Object obj) {
				com.ctrip.hermes.metaservice.model.MonitorEvent event = (com.ctrip.hermes.metaservice.model.MonitorEvent) obj;
				return parse(event);
			}
		});
	}
}
