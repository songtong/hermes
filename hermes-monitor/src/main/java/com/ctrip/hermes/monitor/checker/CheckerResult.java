package com.ctrip.hermes.monitor.checker;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class CheckerResult {
	private boolean m_runSuccess = true;

	private String m_errorMessage;

	private Exception m_exception;

	private List<MonitorEvent> m_monitorEvents = new LinkedList<>();

	public Exception getException() {
		return m_exception;
	}

	public void setException(Exception exception) {
		m_exception = exception;
	}

	public boolean isRunSuccess() {
		return m_runSuccess;
	}

	public void setRunSuccess(boolean runSuccess) {
		m_runSuccess = runSuccess;
	}

	public String getErrorMessage() {
		return m_errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		m_errorMessage = errorMessage;
	}

	public List<MonitorEvent> getMonitorEvents() {
		return m_monitorEvents;
	}

	public void addMonitorEvent(MonitorEvent monitorEvent) {
		m_monitorEvents.add(monitorEvent);
	}

}
