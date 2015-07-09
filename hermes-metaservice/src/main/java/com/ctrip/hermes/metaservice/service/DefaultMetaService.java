package com.ctrip.hermes.metaservice.service;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	protected static final Logger m_logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	protected MetaDao m_metaDao;

	@Inject
	protected ZookeeperService m_zookeeperService;

	@Override
	public Meta findLatestMeta() throws DalException {
		com.ctrip.hermes.metaservice.model.Meta dalMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
		if (dalMeta == null) {
			dalMeta = new com.ctrip.hermes.metaservice.model.Meta();
			dalMeta.setValue(JSON.toJSONString(new Meta()));
		}
		return JSON.parseObject(dalMeta.getValue(), Meta.class);
	}

	@Override
	public synchronized boolean updateMeta(Meta meta) throws DalException {
		Meta latest = findLatestMeta();
		if (!latest.getVersion().equals(meta.getVersion())) {
			String e = String.format("Outdated Version. Latest: %s, Offered: %s", latest.getVersion(), meta.getVersion());
			throw new RuntimeException(e);
		}

		com.ctrip.hermes.metaservice.model.Meta dalMeta = new com.ctrip.hermes.metaservice.model.Meta();
		try {
			meta.setVersion(meta.getVersion() + 1);
			dalMeta.setValue(JSON.toJSONString(meta));
			dalMeta.setDataChangeLastTime(new Date(System.currentTimeMillis()));
			m_metaDao.insert(dalMeta);
			m_zookeeperService.updateZkBaseMetaVersion(meta.getVersion());
		} catch (Exception e) {
			m_logger.warn("Update meta failed", e);
			throw new RuntimeException("Update meta failed.", e);
		}
		return true;
	}

}
