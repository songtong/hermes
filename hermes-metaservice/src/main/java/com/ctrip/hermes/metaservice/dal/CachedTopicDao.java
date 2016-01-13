package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedTopicDao extends TopicDao implements CachedDao<Long, Topic> {

	private Cache<Long, Topic> cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).maximumSize(500)
	      .build();

	private Cache<String, Topic> nameCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
	      .maximumSize(500).build();

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Topic proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Topic findByPK(final Long keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Topic>() {

				@Override
				public Topic call() throws Exception {
					return findByPK(keyId, TopicEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Topic findByName(final String name) throws DalException {
		try {
			return nameCache.get(name, new Callable<Topic>() {

				@Override
				public Topic call() throws Exception {
					return findByName(name, TopicEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Topic proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Topic> list() throws DalException {
		if (isNeedReload) {
			List<Topic> models = list(TopicEntity.READSET_FULL);
			for (Topic model : models) {
				cache.put(model.getKeyId(), model);
			}
			if (models.size() > cache.size()) {
				cache = CacheBuilder.newBuilder().maximumSize(models.size() * 2).build();
				for (Topic model : models) {
					cache.put(model.getKeyId(), model);
				}
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Topic proto, Updateset<Topic> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

}
