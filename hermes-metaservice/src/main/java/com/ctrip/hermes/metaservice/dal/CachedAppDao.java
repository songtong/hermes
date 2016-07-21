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

import com.ctrip.hermes.metaservice.model.App;
import com.ctrip.hermes.metaservice.model.AppDao;
import com.ctrip.hermes.metaservice.model.AppEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedAppDao extends AppDao implements CachedDao<Long, App> {

	private int max_size = 10;

	private LoadingCache<Long, App> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, App>() {

		      @Override
		      public App load(Long key) throws Exception {
			      return findByPK(key, AppEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(App proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public App findByPK(final Long keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedAppDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	public int insert(App proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

	public Collection<App> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<App> models = list(AppEntity.READSET_FULL);
			for (App model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(App proto, Updateset<App> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

}
