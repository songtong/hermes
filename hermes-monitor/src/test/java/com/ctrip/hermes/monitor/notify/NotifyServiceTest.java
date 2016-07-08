package com.ctrip.hermes.monitor.notify;

import java.util.Date;

import org.junit.Test;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.monitor.checker.notification.LargeBacklogReportMailContent;

public class NotifyServiceTest {
	@Test
	public void test() {
		ConsumeLargeBacklogEvent e = new ConsumeLargeBacklogEvent();
		e.setCreateTime(new Date());
		e.setTotalBacklog(1234);
		ConsumerGroupView c = new ConsumerGroupView();
		c.setTopicName("hello world");
		c.setName("hello kitty");
		c.setPhone1("bbbb");
		LargeBacklogReportMailContent mail = new LargeBacklogReportMailContent();
		mail.addMonitorEvent(e, c);
		NotifyService service = PlexusComponentLocator.lookup(NotifyService.class);
		service.notify(new HermesNotice("XXXX@XXXX", mail));
	}
}
