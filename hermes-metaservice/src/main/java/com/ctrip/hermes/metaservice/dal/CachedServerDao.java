package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.ServerDao;
import com.ctrip.hermes.metaservice.model.ServerEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedServerDao extends ServerDao implements CachedDao<String, Server> {

	private Cache<String, Server> cache = CacheBuilder.newBuilder().maximumSize(100).build();

	@Override
	public int deleteByPK(Server proto) throws DalException {
		cache.invalidateAll();
		return super.deleteByPK(proto);
	}

	public Server findByPK(final String keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Server>() {

				@Override
				public Server call() throws Exception {
					return findByPK(keyId, ServerEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Server proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<Server> list() throws DalException {
		if (cache.size() == 0) {
			List<Server> models = list(ServerEntity.READSET_FULL);
			for (Server model : models) {
				cache.put(model.getKeyId(), model);
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Server proto, Updateset<Server> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
