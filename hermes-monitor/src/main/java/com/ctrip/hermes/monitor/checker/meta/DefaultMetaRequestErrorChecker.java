package com.ctrip.hermes.monitor.checker.meta;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.metaservice.monitor.event.MetaRequestErrorEvent;
import com.ctrip.hermes.metaservice.monitor.event.MetaRequestErrorEvent.MetaRequestErrorType;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;

//@Component(value = DefaultMetaRequestErrorChecker.ID)
public class DefaultMetaRequestErrorChecker extends AbstractMetaRequestErrorChecker {
	public static final String ID = "DefaultMetaRequestErrorChecker";

	private static final List<String> CHECKLIST = Arrays.asList("/meta", "/meta/complete", "/metaserver/servers");

	@Override
	public String name() {
		return ID;
	}

	@Override
	protected List<String> getUrlChecklist() {
		return CHECKLIST;
	}

	@Override
	protected int getTimeoutMillisecondLimit() {
		return m_config.getMetaRequestTimeoutMillisecondLimit();
	}

	@Override
	protected int getTimeoutCountLimit() {
		return m_config.getMetaRequestTimeoutCountLimit();
	}

	@Override
	protected int getErrorCountLimit() {
		return m_config.getMetaRequestErrorCountLimit();
	}

	@Override
	protected MonitorEvent generateEvent(String name, String meta, MetaRequestErrorType type, int count) {
		return new MetaRequestErrorEvent(name, meta, type, count);
	}

}
