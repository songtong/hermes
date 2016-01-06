package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Endpoint;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedEndpointDao extends EndpointDao implements CachedDao<String, Endpoint> {

	private Cache<String, Endpoint> cache = CacheBuilder.newBuilder().maximumSize(100).build();

	@Override
	public int deleteByPK(Endpoint proto) throws DalException {
		cache.invalidateAll();
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

	public int insert(Endpoint proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<Endpoint> list() throws DalException {
		if (cache.size() == 0) {
			List<Endpoint> models = list(EndpointEntity.READSET_FULL);
			for (Endpoint model : models) {
				cache.put(model.getKeyId(), model);
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Endpoint proto, Updateset<Endpoint> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
