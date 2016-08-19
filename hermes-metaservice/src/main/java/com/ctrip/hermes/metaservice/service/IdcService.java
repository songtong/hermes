package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.model.Idc;
import com.ctrip.hermes.metaservice.model.IdcDao;
import com.ctrip.hermes.metaservice.model.IdcEntity;

@Named
public class IdcService {

	private static final Logger m_logger = LoggerFactory.getLogger(IdcService.class);

	@Inject
	protected IdcDao m_idcDao;

	@Inject
	private TransactionManager m_tm;

	public List<Idc> listIdcs() throws DalException {
		return m_idcDao.list(IdcEntity.READSET_FULL);
	}

	public Idc findIdc(int idc) throws DalException {
		List<Idc> idcs = m_idcDao.findById(idc, IdcEntity.READSET_FULL);
		if (idcs.isEmpty()) {
			return null;
		}
		return idcs.get(0);
	}

	public void addIdc(Idc idc) throws DalException {
		m_idcDao.insert(idc);
	}

	public void deleteIdc(int idcId) throws DalException {
		Idc idc = new Idc();
		idc.setId(idcId);
		m_idcDao.deleteByPK(idc);
	}

	public void updateIdc(Idc idc) throws DalException {
		m_idcDao.updateByPK(idc, IdcEntity.UPDATESET_FULL);
	}

	public void switchPrimary(int idcId) throws Exception {
		m_tm.startTransaction("fxhermesmetadb");
		try {
			List<Idc> primaryIdcs = m_idcDao.findByPrimaryStatus(true, IdcEntity.READSET_FULL);
			if (primaryIdcs.size() != 1) {
				m_logger.error("Existing {} idc(s) in db! Correct primary idc count should be 1.", primaryIdcs.size());
				throw new IllegalStateException(String.format(
				      "Existing %s idc(s) in db! Correct primary idc count should be 1.", primaryIdcs.size()));
			}

			for (Idc idc : primaryIdcs) {
				m_idcDao.setNotPrimaryById(idc, IdcEntity.UPDATESET_FULL);
			}

			Idc idc = new Idc();
			idc.setId(idcId);
			m_idcDao.setPrimaryById(idc, IdcEntity.UPDATESET_FULL);
			m_tm.commitTransaction();
		} catch (Exception e) {
			m_tm.rollbackTransaction();
			m_logger.error("Failed to switch primary idc to {}", idcId, e);
			throw e;
		}
	}

	public void forceSwitchPrimary(int idcId) throws Exception {
		m_tm.startTransaction("fxhermesmetadb");
		try {
			List<Idc> primaryIdcs = m_idcDao.findByPrimaryStatus(true, IdcEntity.READSET_FULL);

			for (Idc idc : primaryIdcs) {
				m_idcDao.setNotPrimaryById(idc, IdcEntity.UPDATESET_FULL);
			}

			Idc idc = new Idc();
			idc.setId(idcId);
			m_idcDao.setPrimaryById(idc, IdcEntity.UPDATESET_FULL);
			m_tm.commitTransaction();
		} catch (Exception e) {
			m_tm.rollbackTransaction();
			m_logger.error("Failed to switch primary idc to {}", idcId, e);
			throw e;
		}
	}

	public void enableIdc(int idcId) throws DalException {
		Idc idc = new Idc();
		idc.setId(idcId);
		idc.setEnabled(true);
		m_idcDao.setEnabledStatusById(idc, IdcEntity.UPDATESET_FULL);
	}

	public void disableIdc(int idcId) throws DalException {
		Idc idc = new Idc();
		idc.setId(idcId);
		idc.setEnabled(false);
		m_idcDao.setEnabledStatusById(idc, IdcEntity.UPDATESET_FULL);
	}

	public List<com.ctrip.hermes.meta.entity.Idc> listIdcEntities() throws DalException {
		List<com.ctrip.hermes.meta.entity.Idc> idcEntyties = new ArrayList<>();
		for (Idc idc : m_idcDao.list(IdcEntity.READSET_FULL)) {
			idcEntyties.add(ModelToEntityConverter.convert(idc));
		}
		return idcEntyties;
	}
}
