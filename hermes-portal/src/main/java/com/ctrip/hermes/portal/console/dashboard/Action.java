package com.ctrip.hermes.portal.console.dashboard;

public enum Action implements org.unidal.web.mvc.Action {
	TOPIC("topic"), //
	CLIENT("client"), //
	BROKER("broker"), //
	TOPIC_DETAIL("topic-detail"), //
	BROKER_DETAIL("broker-detail"), //
	BROKER_DETAIL_HOME("broker-detail-home");

	private String m_name;

	private Action(String name) {
		m_name = name;
	}

	public static Action getByName(String name, Action defaultAction) {
		for (Action action : Action.values()) {
			if (action.getName().equals(name)) {
				return action;
			}
		}

		return defaultAction;
	}

	@Override
	public String getName() {
		return m_name;
	}
}
