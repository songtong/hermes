package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedConsumerGroupDao extends ConsumerGroupDao implements CachedDao<Integer, ConsumerGroup> {

	private Cache<Integer, ConsumerGroup> cache = CacheBuilder.newBuilder().maximumSize(100).build();

	@Override
	public int deleteByPK(ConsumerGroup proto) throws DalException {
		cache.invalidateAll();
		return super.deleteByPK(proto);
	}

	public ConsumerGroup findByPK(final Integer keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<ConsumerGroup>() {

				@Override
				public ConsumerGroup call() throws Exception {
					return findByPK(keyId, ConsumerGroupEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(ConsumerGroup proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<ConsumerGroup> list() throws DalException {
		if (cache.size() == 0) {
			List<ConsumerGroup> models = list(ConsumerGroupEntity.READSET_FULL);
			for (ConsumerGroup model : models) {
				cache.put(model.getKeyId(), model);
			}
			if (models.size() > cache.size()) {
				cache = CacheBuilder.newBuilder().maximumSize(models.size() * 2).build();
				for (ConsumerGroup model : models) {
					cache.put(model.getKeyId(), model);
				}
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(ConsumerGroup proto, Updateset<ConsumerGroup> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
