package com.ctrip.hermes.metaservice.dal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Producer;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.ProducerEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedProducerDao extends ProducerDao implements CachedDao<Long, Producer> {

	private int max_size = 5;
	
	private LoadingCache<Long, List<Producer>> topicCache = CacheBuilder.newBuilder().maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, List<Producer>>() {

		      @Override
		      public List<Producer> load(Long key) throws Exception {
			      return findByTopicId(key, ProducerEntity.READSET_FULL);
		      }

	      });

	@Override
	public Producer findByPK(Long key) throws DalException {
		return new Producer();
	}

	public List<Producer> findByTopic(final Long keyType) throws DalException {
		try {
			return topicCache.get(keyType);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedProducerDao.class.getSimpleName() + "_topicCache", topicCache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		topicCache.invalidateAll();
	}

	public Collection<Producer> list(boolean fromDB) throws DalException {
		return new ArrayList<Producer>();
	}

}
