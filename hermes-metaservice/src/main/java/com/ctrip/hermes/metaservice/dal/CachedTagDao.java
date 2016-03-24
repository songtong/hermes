package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.metaservice.model.Tag;
import com.ctrip.hermes.metaservice.model.TagDao;
import com.ctrip.hermes.metaservice.model.TagEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

public class CachedTagDao extends TagDao implements CachedDao<Long, Tag> {
	private int maxSize = 1000;
	
	private LoadingCache<Long, Tag> cache = CacheBuilder.newBuilder().maximumSize(maxSize).recordStats()
		      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, Tag>() {

			      @Override
			      public Tag load(Long key) throws Exception {
				      return findByPK(key, TagEntity.READSET_FULL);
			      }

		      });
	
	private volatile boolean isNeedReload = true;

	@Override
	public Tag findByPK(Long key) throws DalException {
		try {
			return cache.get(key);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	@Override
	public Collection<Tag> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Tag> models = list(TagEntity.READSET_FULL);
			if (models.size() > maxSize) {
				maxSize = models.size() * 2;
				cache = CacheBuilder.newBuilder().maximumSize(maxSize).recordStats().refreshAfterWrite(10, TimeUnit.MINUTES)
				      .build(new CacheLoader<Long, Tag>() {

					      @Override
					      public Tag load(Long key) throws Exception {
						      return findByPK(key, TagEntity.READSET_FULL);
					      }

				      });
			}
			for (Tag model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedTagDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}
	
}
