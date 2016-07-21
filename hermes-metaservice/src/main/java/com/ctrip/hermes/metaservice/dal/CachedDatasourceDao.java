package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Datasource;
import com.ctrip.hermes.metaservice.model.DatasourceDao;
import com.ctrip.hermes.metaservice.model.DatasourceEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedDatasourceDao extends DatasourceDao implements CachedDao<String, Datasource> {

	private int max_size = 100;

	private LoadingCache<String, Datasource> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Datasource>() {

		      @Override
		      public Datasource load(String key) throws Exception {
			      return findByPK(key, DatasourceEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Datasource proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Datasource findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedDatasourceDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	public int insert(Datasource proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

	public Collection<Datasource> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Datasource> models = list(DatasourceEntity.READSET_FULL);
			cache.invalidateAll();
			for (Datasource model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}
	
	public int updateByPK(Datasource proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, DatasourceEntity.UPDATESET_FULL);
	}

}
