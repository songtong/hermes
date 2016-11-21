package com.ctrip.hermes.admin.core.service;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.converter.ModelToEntityConverter;
import com.ctrip.hermes.admin.core.dal.CachedPartitionDao;
import com.ctrip.hermes.meta.entity.Partition;

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

	public List<com.ctrip.hermes.meta.entity.Partition> findPartitions(com.ctrip.hermes.admin.core.model.Topic topicModel)
	      throws DalException {
		List<com.ctrip.hermes.admin.core.model.Partition> models = partitionDao.findByTopic(topicModel.getId(), false);
		List<com.ctrip.hermes.meta.entity.Partition> entities = new ArrayList<>();
		for (com.ctrip.hermes.admin.core.model.Partition model : models) {
			com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public List<Partition> findPartitionsByTopic(Long topicId) {
		List<com.ctrip.hermes.meta.entity.Partition> entities = new ArrayList<>();
		try {
			List<com.ctrip.hermes.admin.core.model.Partition> models = partitionDao.findByTopic(topicId, false);
			for (com.ctrip.hermes.admin.core.model.Partition model : models) {
				com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
				entities.add(entity);
			}
		} catch (DalException e) {
			e.printStackTrace();
		}
		return entities;
	}
}
