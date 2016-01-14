package com.ctrip.hermes.metaservice.dal;

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

import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;

@Named
public class CachedTopicDao extends TopicDao implements CachedDao<Long, Topic> {

	private Cache<Long, Topic> cache = CacheBuilder.newBuilder().maximumSize(1000).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, Topic>() {

		      @Override
		      public Topic load(Long key) throws Exception {
			      return findByPK(key, TopicEntity.READSET_FULL);
		      }

	      });

	private Cache<String, Topic> nameCache = CacheBuilder.newBuilder().maximumSize(1000).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Topic>() {

		      @Override
		      public Topic load(String key) throws Exception {
			      return findByName(key, TopicEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Topic proto) throws DalException {
		cache.invalidate(proto.getId());
		nameCache.invalidate(proto.getName());
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Topic findByPK(final Long keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Topic>() {

				@Override
				public Topic call() throws Exception {
					return findByPK(keyId, TopicEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Topic findByName(final String name) throws DalException {
		try {
			return nameCache.get(name, new Callable<Topic>() {

				@Override
				public Topic call() throws Exception {
					return findByName(name, TopicEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Topic proto) throws DalException {
		cache.invalidateAll();
		nameCache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Topic> list() throws DalException {
		if (isNeedReload) {
			List<Topic> models = list(TopicEntity.READSET_FULL);
			for (Topic model : models) {
				cache.put(model.getKeyId(), model);
				nameCache.put(model.getName(), model);
			}
			if (models.size() > cache.size()) {
				cache = CacheBuilder.newBuilder().maximumSize(models.size() * 2).build();
				for (Topic model : models) {
					cache.put(model.getKeyId(), model);
					nameCache.put(model.getName(), model);
				}
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Topic proto, Updateset<Topic> updateset) throws DalException {
		cache.invalidate(proto.getId());
		nameCache.invalidate(proto.getName());
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedTopicDao.class.getSimpleName() + "_cache", cache.stats());
		result.put(CachedTopicDao.class.getSimpleName() + "_nameCache", nameCache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		nameCache.invalidateAll();
		isNeedReload = true;
	}

}
