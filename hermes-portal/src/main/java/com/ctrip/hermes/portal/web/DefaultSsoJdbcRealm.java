package com.ctrip.hermes.portal.web;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.jdbc.JdbcRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.CollectionUtils;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.dal.permission.RolesPermissions;
import com.ctrip.hermes.portal.dal.permission.RolesPermissionsDao;
import com.ctrip.hermes.portal.dal.permission.RolesPermissionsEntity;
import com.ctrip.hermes.portal.dal.role.UserRoles;
import com.ctrip.hermes.portal.dal.role.UserRolesDao;
import com.ctrip.hermes.portal.dal.role.UserRolesEntity;

public class DefaultSsoJdbcRealm extends JdbcRealm {
	private UserRolesDao userRolesDao;
	private RolesPermissionsDao rolesPermissionsDao;

	@Override
	protected AuthenticationInfo doGetAuthenticationInfo(
			AuthenticationToken token) throws AuthenticationException {
		return new SimpleAccount(token.getPrincipal(), token.getCredentials(), this.getName());
	}

	@Override
	protected AuthorizationInfo doGetAuthorizationInfo(
			PrincipalCollection principals) {

        if (principals == null) {
            throw new AuthorizationException("PrincipalCollection method argument can NOT be null.");
        }
        
        if (userRolesDao == null) {
    		userRolesDao = PlexusComponentLocator.lookup(UserRolesDao.class);
    		rolesPermissionsDao = PlexusComponentLocator.lookup(RolesPermissionsDao.class);
        }

        String username = (String) getAvailablePrincipal(principals);

        Set<String> roleNames = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
        try {
            List<UserRoles> userRoles = userRolesDao.findByUsername(username, UserRolesEntity.READSET_FULL);
            
            for (UserRoles userRole : userRoles) {
            	roleNames.add(userRole.getRoleName());
            	List<RolesPermissions> rolePermissions = rolesPermissionsDao.findByRolename(userRole.getRoleName(), RolesPermissionsEntity.READSET_FULL);
            	for (RolesPermissions rolePermission : rolePermissions) {
            		permissions.add(rolePermission.getPermission());
            	}
            }
        } catch (DalException e) {
            // Rethrow any SQL errors as an authorization exception
            throw new AuthorizationException(e);
        }

        SimpleAuthorizationInfo authorizationInfo = new SimpleAuthorizationInfo(roleNames);
        authorizationInfo.setStringPermissions(permissions);
        return authorizationInfo;
	}
	
    protected Object getAvailablePrincipal(PrincipalCollection principals) {
        Object primary = null;
        if (!CollectionUtils.isEmpty(principals)) {
            Collection thisPrincipals = principals.fromRealm(getName());
            if (!CollectionUtils.isEmpty(thisPrincipals)) {
                primary = thisPrincipals.iterator().next();
            } else {
                //no principals attributed to this particular realm.  Fall back to the 'master' primary:
                primary = principals.getPrimaryPrincipal();
            }
        }

        return primary;
    }
}
