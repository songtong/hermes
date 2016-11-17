package com.ctrip.hermes.monitor.notify;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.hermes.admin.core.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.NotifyService;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.checker.notification.LargeBacklogReportMailContent;

public class NotifyServiceTest {
	@Test
	public void test() throws Exception {
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
		service.registerRateLimiter("xxxx@xxxx", 5, TimeUnit.SECONDS);
		for (int i = 0; i < 100; i++) {
			service.notify(new HermesNotice(Arrays.asList("xxxx@xxxx", ""), mail));
			TimeUnit.SECONDS.sleep(1);
		}
	}
}
