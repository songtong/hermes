package com.ctrip.hermes.monitor.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.BaseCheckerTest;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.job.partition.entity.PartitionInfo;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class PartitionServiceTest {

	@Autowired
	private PartitionService m_service;

	@Test
	public void testModifyPartition() throws Exception {
		int curIdx = 7;
		long curBorder = 800;

		Datasource ds = new Datasource();
		ds.addProperty(new Property("url").setValue("jdbc:mysql://localhost:3306/fxhermesshard01db"));
		ds.addProperty(new Property("user").setValue("root"));
		ds.addProperty(new Property("password").setValue(""));

		Topic t = new Topic().setName("song.test").setId(1L);
		Partition p = new Partition(0);
		MessageTableContext ctx = new MessageTableContext(t, p, 0, 30, 2, 5);
		ctx.setDatasource(ds);

		List<PartitionInfo> addlist = new ArrayList<PartitionInfo>();
		for (int i = 0; i < 5; i++) {
			PartitionInfo info = new PartitionInfo();
			info.setUpperbound(curBorder + (i + 1) * 100);
			info.setName("p" + (curIdx + i + 1));
			addlist.add(info);
		}

		m_service.addPartitions(ctx, addlist);
		m_service.dropPartitions(ctx, addlist);
	}
}
