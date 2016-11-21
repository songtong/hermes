package com.ctrip.hermes.admin.core.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Partition;
import com.ctrip.hermes.admin.core.model.PartitionDao;
import com.ctrip.hermes.admin.core.model.PartitionEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedPartitionDao extends PartitionDao implements CachedDao<Long, Partition> {

	private int max_size = 1000;

	private LoadingCache<Long, List<Partition>> topicCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
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
	public int updateByTopicAndPartition(Partition proto, org.unidal.dal.jdbc.Updateset<Partition> updateset) throws DalException {
		int ret = super.updateByTopicAndPartition(proto, updateset);
		topicCache.invalidate(proto.getTopicId());
		return ret;
	};

	@Override
	public Partition findByPK(Long key) throws DalException {
		return new Partition();
	}

	public List<Partition> findByTopic(final Long keyId, boolean fromDB) throws DalException {
		try {
			if (fromDB) {
				topicCache.invalidate(keyId);
			}
			return topicCache.get(keyId);
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

	public Collection<Partition> list(boolean fromDB) throws DalException {
		// TODO: list depend to parameter[fromDB]
		return list(PartitionEntity.READSET_FULL);
	}
}
