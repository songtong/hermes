package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.App;
import com.ctrip.hermes.metaservice.model.AppDao;
import com.ctrip.hermes.metaservice.model.AppEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedAppDao extends AppDao implements CachedDao<Long, App> {

	private Cache<Long, App> cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).maximumSize(100)
	      .build();

	@Override
	public int deleteByPK(App proto) throws DalException {
		cache.invalidateAll();
		return super.deleteByPK(proto);
	}

	public App findByPK(final Long keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<App>() {

				@Override
				public App call() throws Exception {
					return findByPK(keyId, AppEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(App proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<App> list() throws DalException {
		if (cache.size() == 0) {
			List<App> models = list(AppEntity.READSET_FULL);
			for (App model : models) {
				cache.put(model.getKeyId(), model);
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(App proto, Updateset<App> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
