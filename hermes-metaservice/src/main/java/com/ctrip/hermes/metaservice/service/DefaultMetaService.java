package com.ctrip.hermes.metaservice.service;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.MetaEntity;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService {

	@Inject
	private MetaDao m_metaDao;

	@Override
	public Meta findLatestMeta() throws DalException {
		com.ctrip.hermes.metaservice.model.Meta dalMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
		return JSON.parseObject(dalMeta.getValue(), Meta.class);
	}

}
