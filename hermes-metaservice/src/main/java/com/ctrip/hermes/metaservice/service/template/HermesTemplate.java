package com.ctrip.hermes.metaservice.service.template;

public enum HermesTemplate {
	TEST("test_template.ftl"), //

	LONG_TIME_NO_PRODUCE("alarm_long_time_no_produce.ftl"), //

	LONG_TIME_NO_CONSUME("alarm_long_time_no_consume.ftl"), //

	TOPIC_LARGE_DEADLETTER("alarm_topic_large_deadletter.ftl"), //

	CONSUME_LARGE_BACKLOG("alarm_consume_large_backlog.ftl"), //

	CONSUME_LARGE_DELAY("alarm_consume_large_delay.ftl");

	private String m_templateName;

	private HermesTemplate(String templateName) {
		m_templateName = templateName;
	}

	public String getName() {
		return m_templateName;
	}
}
