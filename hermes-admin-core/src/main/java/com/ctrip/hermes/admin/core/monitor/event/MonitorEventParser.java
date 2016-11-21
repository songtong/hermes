package com.ctrip.hermes.admin.core.monitor.event;

import java.util.List;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;

public class MonitorEventParser {
	public static MonitorEvent parse(com.ctrip.hermes.admin.core.model.MonitorEvent dbEntity) {
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
	public static List<MonitorEvent> parse(List<com.ctrip.hermes.admin.core.model.MonitorEvent> dbEntities) {
		return (List<MonitorEvent>) CollectionUtil.collect(dbEntities, new Transformer() {
			@Override
			public Object transform(Object obj) {
				com.ctrip.hermes.admin.core.model.MonitorEvent event = (com.ctrip.hermes.admin.core.model.MonitorEvent) obj;
				return parse(event);
			}
		});
	}
}
