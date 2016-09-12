package com.ctrip.hermes.portal.service.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.dal.tag.Tag;
import com.ctrip.hermes.portal.topic.TopicView;

public abstract class PartitionStrategy {
	private static final Logger log = LoggerFactory.getLogger(PartitionStrategy.class);

	private static Map<String, PartitionStrategy> strategies = null;

	public static PartitionStrategy getStategy(String storageType) {
		if (strategies != null) {
			return strategies.get(storageType);
		}

		synchronized (PartitionStrategy.class) {
			if (strategies == null) {
				try {
					strategies = ContainerLoader.getDefaultContainer().lookupMap(PartitionStrategy.class);
				} catch (ComponentLookupException e) {
					log.error("Failed to find PartitionStrategy from plexus container", e);
				}
			}
		}
		return strategies.get(storageType);
	}

	static class StrategyDatasource {
		private Pair<String, String> datasource;

		private List<Tag> tags = null;

		public static StrategyDatasource newInstance(Pair<String, String> datasource) {
			StrategyDatasource strategyDatasource = new StrategyDatasource();
			strategyDatasource.setDatasource(datasource);
			return strategyDatasource;
		}

		public static StrategyDatasource newInstance(Pair<String, String> datasource, List<Tag> tags) {
			StrategyDatasource strategyDatasource = StrategyDatasource.newInstance(datasource);
			strategyDatasource.setTags(tags);
			return strategyDatasource;
		}

		private StrategyDatasource() {
		}

		public Pair<String, String> getDatasource() {
			return datasource;
		}

		public void setDatasource(Pair<String, String> datasource) {
			this.datasource = datasource;
		}

		public List<Tag> getTags() {
			return tags;
		}

		public void setTags(List<Tag> tags) {
			this.tags = tags;
		}

		public void addTag(Tag tag) {
			if (this.tags == null) {
				this.tags = new ArrayList<Tag>();
			}

			this.tags.add(tag);
		}
	}

	public TopicView apply(TopicApplication application) {
		TopicView topicView = new TopicView();
		applyStrategy(application, topicView);
		applyDefault(application, topicView);
		return topicView;
	}

	protected abstract StrategyDatasource getDefaultDatasource(TopicApplication application) throws DalException;

	protected void applyStrategy(TopicApplication application, TopicView topicView) {
		int partitionCount = 1;
		if (application.getStorageType().equals("mysql")) {
			if (application.getMaxMsgNumPerDay() >= 20000000) {
				partitionCount = 20;
			} else if (application.getMaxMsgNumPerDay() >= 10000000) {
				partitionCount = 10;
			} else {
				partitionCount = 5;
			}
		}

		StrategyDatasource strategyDatasource;
		try {
			strategyDatasource = getDefaultDatasource(application);
		} catch (DalException e) {
			log.error("Exception encountered when retrieving default datasource", e);
			throw new RuntimeException(e);
		}

		topicView.setTags(strategyDatasource.getTags());

		List<Partition> topicPartitions = new ArrayList<Partition>();
		for (int i = 0; i < partitionCount; i++) {
			Partition p = new Partition();
			p.setReadDatasource(strategyDatasource.getDatasource().getKey());
			p.setWriteDatasource(strategyDatasource.getDatasource().getValue());
			topicPartitions.add(p);
		}
		topicView.setPartitions(topicPartitions);
	}

	protected void applyDefault(TopicApplication application, TopicView topicView) {
		topicView.setStoragePartitionSize(5000000);
		topicView.setOwner1(application.getOwnerName1() + "/" + application.getOwnerEmail1());
		topicView.setOwner2(application.getOwnerName2() + "/" + application.getOwnerEmail2());
		topicView.setPhone1(application.getOwnerPhone1());
		topicView.setPhone2(application.getOwnerPhone2());
		topicView.setName(application.getProductLine() + "." + application.getEntity() + "." + application.getEvent());
		topicView.setStorageType(application.getStorageType());
		topicView.setCodecType(application.getCodecType());
		topicView.setConsumerRetryPolicy("3:[3,3000]");
		topicView.setAckTimeoutSeconds(5);
		topicView.setStoragePartitionCount(3);
		topicView.setResendPartitionSize(topicView.getStoragePartitionSize() / 10);
		topicView.setDescription(application.getDescription());
		topicView.setPriorityMessageEnabled(application.isPriorityMessageEnabled());
	}
}
