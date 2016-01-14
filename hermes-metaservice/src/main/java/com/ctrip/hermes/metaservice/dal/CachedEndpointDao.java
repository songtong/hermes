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

import com.ctrip.hermes.metaservice.model.Endpoint;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;

@Named
public class CachedEndpointDao extends EndpointDao implements CachedDao<String, Endpoint> {

	private Cache<String, Endpoint> cache = CacheBuilder.newBuilder().maximumSize(10).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Endpoint>() {

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
			return cache.get(keyId, new Callable<Endpoint>() {

				@Override
				public Endpoint call() throws Exception {
					return findByPK(keyId, EndpointEntity.READSET_FULL);
				}

			});
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

	public Collection<Endpoint> list() throws DalException {
		if (isNeedReload) {
			List<Endpoint> models = list(EndpointEntity.READSET_FULL);
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
