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
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;

@Named
public class CachedConsumerGroupDao extends ConsumerGroupDao implements CachedDao<Integer, ConsumerGroup> {

	private Cache<Integer, ConsumerGroup> cache = CacheBuilder.newBuilder().maximumSize(1000).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Integer, ConsumerGroup>() {

		      @Override
		      public ConsumerGroup load(Integer key) throws Exception {
			      return findByPK(key, ConsumerGroupEntity.READSET_FULL);
		      }

	      });

	private Cache<Long, List<ConsumerGroup>> topicCache = CacheBuilder.newBuilder().maximumSize(1000).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, List<ConsumerGroup>>() {

		      @Override
		      public List<ConsumerGroup> load(Long key) throws Exception {
			      return findByTopicId(key, ConsumerGroupEntity.READSET_FULL);
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
			return cache.get(keyId, new Callable<ConsumerGroup>() {

				@Override
				public ConsumerGroup call() throws Exception {
					return findByPK(keyId, ConsumerGroupEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public List<ConsumerGroup> findByTopic(final Long topicId) throws DalException {
		try {
			return topicCache.get(topicId, new Callable<List<ConsumerGroup>>() {
				@Override
				public List<ConsumerGroup> call() throws Exception {
					return findByTopicId(topicId, ConsumerGroupEntity.READSET_FULL);
				}
			});
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

	public Collection<ConsumerGroup> list() throws DalException {
		if (isNeedReload) {
			List<ConsumerGroup> models = list(ConsumerGroupEntity.READSET_FULL);
			for (ConsumerGroup model : models) {
				cache.put(model.getKeyId(), model);
				List<ConsumerGroup> cgs = topicCache.getIfPresent(model.getTopicId());
				if (cgs == null) {
					cgs = new ArrayList<ConsumerGroup>();
					topicCache.put(model.getTopicId(), cgs);
				}
				cgs.add(model);
			}
			if (models.size() > cache.size()) {
				cache = CacheBuilder.newBuilder().maximumSize(models.size() * 2).build();
				for (ConsumerGroup model : models) {
					cache.put(model.getKeyId(), model);
					List<ConsumerGroup> cgs = topicCache.getIfPresent(model.getTopicId());
					if (cgs == null) {
						cgs = new ArrayList<ConsumerGroup>();
						topicCache.put(model.getTopicId(), cgs);
					}
					cgs.add(model);
				}
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
