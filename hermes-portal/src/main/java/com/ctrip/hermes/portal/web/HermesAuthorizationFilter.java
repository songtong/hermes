package com.ctrip.hermes.portal.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;

public class HermesAuthorizationFilter implements Filter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {

	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		Subject subject = SecurityUtils.getSubject();
		if (subject.hasRole("admin")) {
			System.out.println("done");
		} else {
			System.out.println("fail");
		}
		chain.doFilter(request, response);
	}

	@Override
	public void destroy() {
	}
}
