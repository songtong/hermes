package com.ctrip.hermes.admin.core.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedTopicDao extends TopicDao implements CachedDao<Long, Topic> {

	private int max_size = 1000;

	private LoadingCache<Long, Topic> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, Topic>() {

		      @Override
		      public Topic load(Long key) throws Exception {
			      return findByPK(key, TopicEntity.READSET_FULL);
		      }

	      });

	private LoadingCache<String, Topic> nameCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
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
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Topic findByName(final String name) throws DalException {
		try {
			return nameCache.get(name);
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

	public Collection<Topic> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Topic> models = list(TopicEntity.READSET_FULL);
			if (models.size() > max_size) {
				max_size = models.size() * 2;
				cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats().refreshAfterWrite(10, TimeUnit.MINUTES)
				      .build(new CacheLoader<Long, Topic>() {

					      @Override
					      public Topic load(Long key) throws Exception {
						      return findByPK(key, TopicEntity.READSET_FULL);
					      }

				      });
				nameCache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats().refreshAfterWrite(10, TimeUnit.MINUTES)
				      .build(new CacheLoader<String, Topic>() {

					      @Override
					      public Topic load(String key) throws Exception {
						      return findByName(key, TopicEntity.READSET_FULL);
					      }

				      });
			}else{
				cache.invalidateAll();
				nameCache.invalidateAll();
			}
			
			for (Topic model : models) {
				cache.put(model.getKeyId(), model);
				nameCache.put(model.getName(), model);
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
