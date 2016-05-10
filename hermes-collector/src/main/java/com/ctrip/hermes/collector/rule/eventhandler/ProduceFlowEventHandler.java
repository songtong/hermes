package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.ProduceFlowState;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;

@Component
@TriggeredBy(ProduceFlowState.class)
@EPL("select * from ProduceFlowState")
public class ProduceFlowEventHandler extends RuleEventHandler {

	@Override
	public boolean validate(RuleEvent event) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		// TODO Auto-generated method stub
		return null;
	}

}
