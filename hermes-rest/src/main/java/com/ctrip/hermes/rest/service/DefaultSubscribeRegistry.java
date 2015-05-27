package com.ctrip.hermes.rest.service;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Named;

@Named(type = SubscribeRegistry.class)
public class DefaultSubscribeRegistry implements SubscribeRegistry {

	@Override
	public List<Subscription> getSubscriptions() {
		// TODO Auto-generated method stub
		Subscription sub = new Subscription();
		sub.setGroupId("group1");
		sub.setPushHttpUrls(Arrays.asList("http://127.0.0.1:1357/testpush"));
		sub.setTopic("order_new");

		return Arrays.asList(sub);
	}

}
