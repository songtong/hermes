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

import com.ctrip.hermes.admin.core.model.Endpoint;
import com.ctrip.hermes.admin.core.model.EndpointDao;
import com.ctrip.hermes.admin.core.model.EndpointEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedEndpointDao extends EndpointDao implements CachedDao<String, Endpoint> {

	private int max_size = 500;

	private LoadingCache<String, Endpoint> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size)
	      .recordStats().refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Endpoint>() {

		      @Override
		      public Endpoint load(String key) throws Exception {
			      return findByPK(key, EndpointEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Endpoint proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Endpoint findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedEndpointDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	public int insert(Endpoint proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

	public Collection<Endpoint> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Endpoint> models = list(EndpointEntity.READSET_FULL);
			cache.invalidateAll();
			for (Endpoint model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Endpoint proto, Updateset<Endpoint> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

}
