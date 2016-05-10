package com.ctrip.hermes.collector.hub;

import java.util.ArrayList;

import org.junit.Test;
import static org.mockito.Mockito.*;

public class Mockito {
	@Test
	public void test() {
		ArrayList list = mock(ArrayList.class);
		when(list.get(anyInt())).thenReturn("hellworold");
		
		System.out.println(list.get(3));
		
		ArrayList l = new ArrayList();
		l = spy(l);
		when(l.get(anyInt())).thenReturn("dead");
		System.out.println(l.get(0));
	}
}
