package com.ctrip.hermes.metaservice.dal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Partition;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedPartitionDao extends PartitionDao implements CachedDao<Long, Partition> {

	private Cache<Long, List<Partition>> topicCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
	      .maximumSize(100).build();

	public int deleteByTopic(Partition proto) throws DalException {
		topicCache.invalidate(proto.getTopicId());
		return super.deleteByTopicId(proto);
	}

	public List<Partition> findByTopic(final Long keyId) throws DalException {
		try {
			return topicCache.get(keyId, new Callable<List<Partition>>() {

				@Override
				public List<Partition> call() throws Exception {
					return findByTopicId(keyId, PartitionEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Partition proto) throws DalException {
		topicCache.invalidate(proto.getTopicId());
		return super.insert(proto);
	}

	public Collection<Partition> list() throws DalException {
		return new ArrayList<Partition>();
	}

	@Override
   public Partition findByPK(Long key) throws DalException {
	   return new Partition();
   }

   public int deleteByTopicId(Partition proto) throws DalException {
   	topicCache.invalidate(proto.getTopicId());
   	return super.deleteByTopicId(proto);
   }
}
