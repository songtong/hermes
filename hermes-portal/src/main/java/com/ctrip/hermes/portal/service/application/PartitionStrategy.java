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
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.application.TopicApplication;

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
	
	public TopicView apply(TopicApplication application) {
		TopicView topicView = new TopicView();
		applyStrategy(application, topicView);
		applyDefault(application, topicView);
		return topicView;
	}
	
	protected abstract Pair<String, String> getDefaultDatasource(TopicApplication application) throws DalException;
	
	protected void applyStrategy(TopicApplication application, TopicView topicView) {
		int partitionCount = 1;
		if (application.getMaxMsgNumPerDay() >= 20000000) {
			partitionCount = 20;
		} else if (application.getMaxMsgNumPerDay() >= 10000000) {
			partitionCount = 10;
		} else {
			partitionCount = 5;
		}
		
		Pair<String, String> defaultDatasources;
		try {
			defaultDatasources = getDefaultDatasource(application);
		} catch (DalException e) {
			log.error("Exception encountered when retrieving default datasource", e);
			throw new RuntimeException(e);
		}
		List<Partition> topicPartitions = new ArrayList<Partition>();
		for (int i = 0; i < partitionCount; i++) {
			Partition p = new Partition();
			p.setReadDatasource(defaultDatasources.getKey());
			p.setWriteDatasource(defaultDatasources.getValue());
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
	}
}
