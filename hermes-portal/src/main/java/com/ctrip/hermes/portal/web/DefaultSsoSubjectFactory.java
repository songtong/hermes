package com.ctrip.hermes.portal.web;

import org.apache.shiro.mgt.DefaultSubjectFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;
import org.apache.shiro.web.subject.WebSubjectContext;
import org.apache.shiro.web.subject.support.WebDelegatingSubject;
import org.jasig.cas.client.util.AssertionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.dal.user.Users;
import com.ctrip.hermes.portal.dal.user.UsersDao;
import com.ctrip.hermes.portal.dal.user.UsersEntity;

public class DefaultSsoSubjectFactory extends DefaultSubjectFactory {
	private final static Logger LOGGER = LoggerFactory.getLogger(DefaultSsoSubjectFactory.class);
	private UsersDao userDao;
    
	@Override
	public Subject createSubject(SubjectContext context) {
		if (userDao == null) {
			synchronized(DefaultSsoSubjectFactory.class) {
				if (userDao == null) {
					userDao = PlexusComponentLocator.lookup(UsersDao.class);
				}
			}
        }
		
		WebSubjectContext ctx = (WebSubjectContext)context;
        PrincipalCollection principals = context.resolvePrincipals();
        if (principals == null) {
            String username = AssertionHolder.getAssertion().getPrincipal().getName();
	        try {
	        	Users user = userDao.countByPK(username, UsersEntity.READSET_COUNT);
	        	if (user.getCount() == 0) {
	        		user = new Users();
	        		user.setUsername(username);
	        		userDao.insert(user);
	        	}
	            principals = new SimplePrincipalCollection(username, "ctrip");
	        } catch (DalException e) {
	        	LOGGER.error("Failed to verify/insert user {} info from db!", username, e);
	        }
        }

        return new WebDelegatingSubject(principals, true, context.resolveHost(), ctx.getSession(), true, ctx.resolveServletRequest(), ctx.getServletResponse(), context.getSecurityManager());
	}

}
