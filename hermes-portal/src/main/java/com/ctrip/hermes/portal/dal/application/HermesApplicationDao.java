package com.ctrip.hermes.portal.dal.application;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

public interface HermesApplicationDao {

	public List<Application> getAppsByTS(int type, int status) throws DalException;

	public Application getAppById(long id) throws DalException;

	public long saveApplication(Application application) throws DalException;
	
	public List<Application> getApplicationsByOwnerStatus(String owner, int status, int offset, int size) throws DalException;
	
	public int countApplicationsByOwnerStatus(String owner, int status) throws DalException;
	
	public int countApplicationsByStatus(int status) throws DalException;
	
	public Application updateApplication(Application application) throws DalException;
}
