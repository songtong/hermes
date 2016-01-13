package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Datasource;
import com.ctrip.hermes.metaservice.model.DatasourceDao;
import com.ctrip.hermes.metaservice.model.DatasourceEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedDatasourceDao extends DatasourceDao implements CachedDao<String, Datasource> {

	private Cache<String, Datasource> cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
	      .maximumSize(100).build();

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Datasource proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Datasource findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Datasource>() {

				@Override
				public Datasource call() throws Exception {
					return findByPK(keyId, DatasourceEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Datasource proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Datasource> list() throws DalException {
		if (isNeedReload) {
			List<Datasource> models = list(DatasourceEntity.READSET_FULL);
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
