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

import com.ctrip.hermes.metaservice.model.Storage;
import com.ctrip.hermes.metaservice.model.StorageDao;
import com.ctrip.hermes.metaservice.model.StorageEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedStorageDao extends StorageDao implements CachedDao<String, Storage> {

	private int max_size = 10;
	
	private LoadingCache<String, Storage> cache = CacheBuilder.newBuilder().maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Storage>() {

		      @Override
		      public Storage load(String key) throws Exception {
			      return findByPK(key, StorageEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Storage proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Storage findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Storage proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Storage> list() throws DalException {
		if (isNeedReload) {
			List<Storage> models = list(StorageEntity.READSET_FULL);
			for (Storage model : models) {
				cache.put(model.getKeyType(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Storage proto, Updateset<Storage> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedStorageDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

}
