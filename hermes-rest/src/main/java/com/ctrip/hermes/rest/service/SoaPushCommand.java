package com.ctrip.hermes.rest.service;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

public class SoaPushCommand extends HystrixCommand<Object> {

	protected SoaPushCommand(HystrixCommandGroupKey group) {
	   super(group);
   }

	@Override
	protected Object run() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
