package com.ctrip.hermes.collector.rule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
	private static ExecutorService service = Executors.newSingleThreadExecutor();
	private static int counter = 0;

	public static void main(String args[]) {
		for (int index = 0; index < 10; index++) {
			System.out.println(service.submit(new Runnable() {
				@Override
				public void run() {
					for (int index = 0; index < 10; index++) {
						System.out.println("helloworld" + (counter++));
					}
				}
			}));
		}
		
		System.out.println(6 & 3);
	}
}
