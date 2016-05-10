package com.ctrip.hermes.admin.core.service.template;

public enum HermesTemplate {
	TEST("test_template.ftl"), //

	LONG_TIME_NO_PRODUCE("alarm_long_time_no_produce.ftl"), //
	
	LONG_TIME_NO_PRODUCE_REPORT("long_time_no_produce_report.ftl"), //

	LONG_TIME_NO_CONSUME("alarm_long_time_no_consume.ftl"), //
	
	LONG_TIME_NO_CONSUME_REPORT("long_time_no_consume_report.ftl"), //

	TOPIC_LARGE_DEADLETTER("alarm_topic_large_deadletter.ftl"), //
	
	TOPIC_LARGE_DEADLETTER_REPORT("topic_consumer_deadletter_report.ftl"),

	CONSUME_LARGE_BACKLOG("alarm_consume_large_backlog.ftl"), //
	
	CONSUME_LARGE_BACKLOG_REPORT_V2("consume_large_backlog_report.ftl"), //
	
	PRODUCE_LARGE_LATENCY_REPORT("produce_large_latency_report.ftl"), //

	SERVER_ERROR("alarm_server_error.ftl"), //

	CONSUME_LARGE_BACKLOG_REPORT("report_consume_large_backlog.ftl"), //

	CONSUME_LARGE_DELAY("alarm_consume_large_delay.ftl"),
	
	BROKER_COMMAND_DROP("alarm_command_drop.ftl"),
	
	POTENTIAL_ISSUE_NOTICE("potential_issue_notice.ftl"),
	
	TOPIC_FLOW_DAILY_REPORT("topic_flow_daily_report.ftl");

	private String m_templateName;

	private HermesTemplate(String templateName) {
		m_templateName = templateName;
	}

	public String getName() {
		return m_templateName;
	}
}
