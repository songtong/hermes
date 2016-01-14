package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;

@Named
public class PartitionService {

	@Inject
	private CachedPartitionDao partitionDao;

	public Partition findPartition(Long topicId, int partitionId) {
		List<Partition> partitions = findPartitionsByTopic(topicId);
		for (Partition partition : partitions) {
			if (partitionId == partition.getId()) {
				return partition;
			}
		}
		return null;
	}

	public List<com.ctrip.hermes.meta.entity.Partition> findPartitions(
	      com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException {
		List<com.ctrip.hermes.metaservice.model.Partition> models = partitionDao.findByTopic(topicModel.getId());
		List<com.ctrip.hermes.meta.entity.Partition> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Partition model : models) {
			com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public List<Partition> findPartitionsByTopic(Long topicId) {
		List<com.ctrip.hermes.meta.entity.Partition> entities = new ArrayList<>();
		try {
			List<com.ctrip.hermes.metaservice.model.Partition> models = partitionDao.findByTopic(topicId);
			for (com.ctrip.hermes.metaservice.model.Partition model : models) {
				com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
				entities.add(entity);
			}
		} catch (DalException e) {
			e.printStackTrace();
		}
		return entities;
	}
}
