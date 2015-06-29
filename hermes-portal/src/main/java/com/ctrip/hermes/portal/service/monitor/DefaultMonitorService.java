package com.ctrip.hermes.portal.service.monitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;

@Named(type = MonitorService.class, value = DefaultMonitorService.ID)
public class DefaultMonitorService implements MonitorService, Initializable {
	public static final String ID = "default-monitor-service";

	@Inject
	private PortalMetaService m_metaService;

	private Map<String, Datasource> m_dss;

	private class InfoFetchControlTask implements Runnable {

		@Override
		public void run() {
			for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
				Topic topic = entry.getValue();
				for (Partition partition : m_metaService.findPartitionsByTopic(topic.getName())) {
	            
            }
			}
		}

	}

	private void updateDatasources() {
		Map<String, Datasource> dss = new HashMap<String, Datasource>();
		for (Datasource ds : m_metaService.getStorages().get(Storage.MYSQL).getDatasources()) {
			dss.put(ds.getId(), ds);
		}
		m_dss = dss;
	}

	private class InfoFetchTask implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("MONITOR_FETCH_CONTROL", true))
		      .scheduleAtFixedRate(new InfoFetchControlTask(), 0, 1, TimeUnit.MICROSECONDS);
	}
}
