package com.ctrip.hermes.monitor.checker;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.monitor.job.partition.PartitionManagementJob;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class PartitionCheckerTest extends BaseCheckerTest {

	@Component("MockPartitionChecker")
	public static class MockPartitionManagementJob extends PartitionManagementJob {
		@Override
		protected Meta fetchMeta() {
			try {
				return DefaultSaxParser.parse(new String(Files.readAllBytes(Paths.get(this.getClass()
				      .getResource("DBCheckerMockMeta.xml").toURI()))));
			} catch (Exception e) {
				return null;
			}
		}
	}

	@Autowired
	@Qualifier("MockPartitionChecker")
	private MockPartitionManagementJob m_job;

	@Test
	public void testChecker() {
		CheckerResult result = m_job.check().getPartitionChangeListResult();
		List<MonitorEvent> events = result.getMonitorEvents();
		for (MonitorEvent event : events) {
			System.out.println(event);
		}
		Exception e = result.getException();
		if (e != null) {
			e.printStackTrace();
		}
	}
}
