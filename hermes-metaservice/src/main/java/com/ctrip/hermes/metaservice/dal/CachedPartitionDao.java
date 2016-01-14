package com.ctrip.hermes.metaservice.dal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;

@Named
public class CachedPartitionDao extends PartitionDao implements CachedDao<Long, Partition> {

	private Cache<Long, List<Partition>> topicCache = CacheBuilder.newBuilder().maximumSize(1000).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, List<Partition>>() {

		      @Override
		      public List<Partition> load(Long key) throws Exception {
			      return findByTopicId(key, PartitionEntity.READSET_FULL);
		      }

	      });

	public int deleteByTopicId(Partition proto) throws DalException {
		topicCache.invalidate(proto.getTopicId());
		return super.deleteByTopicId(proto);
	}

	@Override
	public Partition findByPK(Long key) throws DalException {
		return new Partition();
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

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedPartitionDao.class.getSimpleName() + "_topicCache", topicCache.stats());
		return result;
	}

	public int insert(Partition proto) throws DalException {
		topicCache.invalidate(proto.getTopicId());
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		topicCache.invalidateAll();
	}

	public Collection<Partition> list() throws DalException {
		return new ArrayList<Partition>();
	}
}
