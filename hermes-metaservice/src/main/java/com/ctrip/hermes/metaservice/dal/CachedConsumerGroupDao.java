package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedConsumerGroupDao extends ConsumerGroupDao implements CachedDao<Integer, ConsumerGroup> {

	private int max_size = 1000;

	private LoadingCache<Integer, ConsumerGroup> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Integer, ConsumerGroup>() {

		      @Override
		      public ConsumerGroup load(Integer key) throws Exception {
			      return findByPK(key, ConsumerGroupEntity.READSET_FULL);
		      }

	      });

	private LoadingCache<Long, Map<Integer, ConsumerGroup>> topicCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size)
	      .recordStats().refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, Map<Integer, ConsumerGroup>>() {

		      @Override
		      public Map<Integer, ConsumerGroup> load(Long key) throws Exception {
			      List<ConsumerGroup> cgs = findByTopicId(key, ConsumerGroupEntity.READSET_FULL);
			      Map<Integer, ConsumerGroup> result = new HashMap<Integer, ConsumerGroup>();
			      for (ConsumerGroup cg : cgs) {
				      result.put(cg.getKeyId(), cg);
			      }
			      return result;
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(ConsumerGroup proto) throws DalException {
		cache.invalidate(proto.getId());
		topicCache.invalidate(proto.getTopicId());
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public ConsumerGroup findByPK(final Integer keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Collection<ConsumerGroup> findByTopic(final Long topicId, boolean fromDB) throws DalException {
		try {
			if (fromDB) {
				topicCache.invalidate(topicId);
			}
			return topicCache.get(topicId).values();
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedConsumerGroupDao.class.getSimpleName() + "_cache", cache.stats());
		result.put(CachedConsumerGroupDao.class.getSimpleName() + "_topicCache", topicCache.stats());
		return result;
	}

	public int insert(ConsumerGroup proto) throws DalException {
		cache.invalidateAll();
		topicCache.invalidate(proto.getTopicId());
		isNeedReload = true;
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		topicCache.invalidateAll();
		isNeedReload = true;
	}

	public Collection<ConsumerGroup> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<ConsumerGroup> models = list(ConsumerGroupEntity.READSET_FULL);
			if (models.size() > max_size) {
				max_size = models.size() * 2;
				cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats().refreshAfterWrite(10, TimeUnit.MINUTES)
				      .build(new CacheLoader<Integer, ConsumerGroup>() {

					      @Override
					      public ConsumerGroup load(Integer key) throws Exception {
						      return findByPK(key, ConsumerGroupEntity.READSET_FULL);
					      }

				      });
				topicCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats().refreshAfterWrite(10, TimeUnit.MINUTES)
				      .build(new CacheLoader<Long, Map<Integer, ConsumerGroup>>() {

					      @Override
					      public Map<Integer, ConsumerGroup> load(Long key) throws Exception {
						      List<ConsumerGroup> cgs = findByTopicId(key, ConsumerGroupEntity.READSET_FULL);
						      Map<Integer, ConsumerGroup> result = new HashMap<Integer, ConsumerGroup>();
						      for (ConsumerGroup cg : cgs) {
							      result.put(cg.getKeyId(), cg);
						      }
						      return result;
					      }

				      });
			}else{
				cache.invalidateAll();
				topicCache.invalidateAll();
			}
			
			for (ConsumerGroup model : models) {
				cache.put(model.getKeyId(), model);
				Map<Integer, ConsumerGroup> cgs = topicCache.getIfPresent(model.getTopicId());
				if (cgs == null) {
					cgs = new HashMap<Integer, ConsumerGroup>();
					topicCache.put(model.getTopicId(), cgs);
				}
				cgs.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(ConsumerGroup proto, Updateset<ConsumerGroup> updateset) throws DalException {
		cache.invalidate(proto.getId());
		topicCache.invalidate(proto.getTopicId());
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

}
