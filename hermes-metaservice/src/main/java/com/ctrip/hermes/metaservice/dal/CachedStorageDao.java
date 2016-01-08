package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Storage;
import com.ctrip.hermes.metaservice.model.StorageDao;
import com.ctrip.hermes.metaservice.model.StorageEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedStorageDao extends StorageDao implements CachedDao<String, Storage> {

	private Cache<String, Storage> cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
	      .maximumSize(100).build();

	@Override
	public int deleteByPK(Storage proto) throws DalException {
		cache.invalidateAll();
		return super.deleteByPK(proto);
	}

	public Storage findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Storage>() {

				@Override
				public Storage call() throws Exception {
					return findByPK(keyId, StorageEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Storage proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<Storage> list() throws DalException {
		if (cache.size() == 0) {
			List<Storage> models = list(StorageEntity.READSET_FULL);
			for (Storage model : models) {
				cache.put(model.getKeyType(), model);
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Storage proto, Updateset<Storage> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
