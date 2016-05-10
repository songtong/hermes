package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.ConsumeFlowState;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;

@Component
@TriggeredBy(ConsumeFlowState.class)
@EPL("select * from ConsumeFlowState")
public class ConsumeFlowEventHandler extends RuleEventHandler {

	@Override
	protected boolean validate(RuleEvent event) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected List<MonitorEvent> doHandleEvent(RuleEvent event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		// TODO Auto-generated method stub
		return null;
	}

}
