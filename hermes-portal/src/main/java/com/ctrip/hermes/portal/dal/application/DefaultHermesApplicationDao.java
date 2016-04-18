package com.ctrip.hermes.portal.dal.application;

import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named(type = HermesApplicationDao.class)
public class DefaultHermesApplicationDao implements HermesApplicationDao {

	@Inject
	private ApplicationDao m_appDao;


	@Override
	public List<Application> getAppsByTS(int type, int status) throws DalException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Application getAppById(long id) throws DalException {
		return m_appDao.findByPK(id, ApplicationEntity.READSET_FULL);
	}

	@Override
	public long saveApplication(Application application) throws DalException {
		m_appDao.insert(application);
		return application.getId();
	}

	@Override
	public Application updateApplication(Application application) throws DalException {
		m_appDao.updateByPK(application, ApplicationEntity.UPDATESET_FULL);
		return application;
	}

	@Override
	public List<Application> getApplicationsByOwnerStatus(String owner, int status, int offset, int size)
			throws DalException {
		if (owner == null) {
			return m_appDao.findByStatus(status, offset, size, ApplicationEntity.READSET_FULL);
		}
		return m_appDao.findByOwnerStatus(owner, status, offset, size, ApplicationEntity.READSET_FULL);
	}

	@Override
	public int countApplicationsByOwnerStatus(String owner, int status)
			throws DalException {
		return m_appDao.countByOwnerStatus(owner, status, ApplicationEntity.READSET_COUNT).getCount();
	}

	@Override
	public int countApplicationsByStatus(int status) throws DalException {
		return m_appDao.countByStatus(status, ApplicationEntity.READSET_COUNT).getCount();
	}
}
