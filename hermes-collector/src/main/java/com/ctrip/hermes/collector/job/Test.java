package com.ctrip.hermes.collector.job;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;

import com.ctrip.hermes.collector.pipe.TopicFlowPipe;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.record.TimeWindowRecord;

public class Test {
	public static void main(String args[]) {
		TopicFlowPipe pipe = new TopicFlowPipe();
		Type superType = pipe.getClass().getGenericSuperclass();
		while (!(superType instanceof ParameterizedType)) {
			superType = ((Class)superType).getGenericSuperclass();
		}
		Type type = ((ParameterizedType)superType).getOwnerType();
		System.out.println(type);
		
		Record<?> r = TimeWindowRecord.newTimeWindowCommand(RecordType.BROKER_ERROR);
	
		Type s = r.getClass().getGenericSuperclass();
		while (!(s instanceof ParameterizedType)) {
			s = ((Class)superType).getGenericSuperclass();
		}
		Type t = ((ParameterizedType)s).getActualTypeArguments()[0];
		System.out.println(type);
		//System.out.println(((ParameterizedType)type).getOwnerType());
		
//		
//		Class<?> clz = pipe.getClass().getSuperclass();
//		superType = ((ParameterizedType)clz.getGenericSuperclass()).getActualTypeArguments()[0];
//		System.out.println(superType);
		
		System.out.println(new Date().toString());
	}
}
