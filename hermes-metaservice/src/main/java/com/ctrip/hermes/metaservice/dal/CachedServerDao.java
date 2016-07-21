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

import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.ServerDao;
import com.ctrip.hermes.metaservice.model.ServerEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedServerDao extends ServerDao implements CachedDao<String, Server> {

	private int max_size = 10;

	private LoadingCache<String, Server> cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Server>() {

		      @Override
		      public Server load(String key) throws Exception {
			      return findByPK(key, ServerEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Server proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Server findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Server proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Server> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Server> models = list(ServerEntity.READSET_FULL);
			for (Server model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Server proto, Updateset<Server> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedServerDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

}
