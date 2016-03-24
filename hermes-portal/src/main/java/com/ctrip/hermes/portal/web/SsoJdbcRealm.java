package com.ctrip.hermes.portal.web;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.realm.jdbc.JdbcRealm;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.portal.dal.user.UsersDao;

@Named(type=JdbcRealm.class)
public class SsoJdbcRealm extends JdbcRealm {
	@Inject
	private UsersDao usersDao;
	
	@Override
	protected AuthenticationInfo doGetAuthenticationInfo(
			AuthenticationToken token) throws AuthenticationException {
		return new SimpleAccount(token.getPrincipal(), token.getCredentials(), this.getName());
	}
}
