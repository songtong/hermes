package com.ctrip.hermes.monitor.checker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.monitor.checker.client.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.client.CatBasedChecker.Timespan;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class CatBasedCheckerTest {

	private static class MockCatBasedChecker extends CatBasedChecker {

		@Override
		public String name() {
			return "MockCatBasedChecker";
		}

		@Override
		protected void doCheck(Timespan timespan, CheckerResult result) throws Exception {
			// do nothing
		}

		@Override
		public Timespan calTimespan(Date toDate, int minutesBefore) {
			return super.calTimespan(toDate, minutesBefore);
		}

	}

	@Test(expected = IllegalArgumentException.class)
	public void testCalTimespanMinutesBeforeLargeThan60() {
		MockCatBasedChecker checker = new MockCatBasedChecker();
		checker.calTimespan(new Date(), 61);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCalTimespanMinutesBeforeSmallerThan0() {
		MockCatBasedChecker checker = new MockCatBasedChecker();
		checker.calTimespan(new Date(), -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCalTimespanMinutesBeforeEqual0() {
		MockCatBasedChecker checker = new MockCatBasedChecker();
		checker.calTimespan(new Date(), 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCalTimespanNoEnoughMinutes() {
		MockCatBasedChecker checker = new MockCatBasedChecker();
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 20);
		checker.calTimespan(calendar.getTime(), 21);
	}

	@Test
	public void testCalTimespanMinuteEqual0() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.MINUTE, 0);

		Calendar expectedCalendar = Calendar.getInstance();
		expectedCalendar.setTime(calendar.getTime());
		expectedCalendar.add(Calendar.HOUR_OF_DAY, -1);

		MockCatBasedChecker checker = new MockCatBasedChecker();

		Timespan timespan = checker.calTimespan(calendar.getTime(), 60);

		assertEquals(sdf.format(expectedCalendar.getTime()), sdf.format(timespan.getStartHour()));

		assertEquals(60, timespan.getMinutes().size());

		for (int i = 0; i <= 59; i++) {
			assertTrue(timespan.getMinutes().contains(i));
		}

		timespan = checker.calTimespan(calendar.getTime(), 32);

		assertEquals(sdf.format(expectedCalendar.getTime()), sdf.format(timespan.getStartHour()));

		assertEquals(32, timespan.getMinutes().size());

		for (int i = 0; i < 32; i++) {
			assertTrue(timespan.getMinutes().contains(59 - i));
		}

	}

	@Test
	public void testCalTimespan() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		Calendar calendar = Calendar.getInstance();
		int currentMinute = 43;
		calendar.set(Calendar.MINUTE, currentMinute);

		MockCatBasedChecker checker = new MockCatBasedChecker();

		int minutesBefore = 5;
		Timespan timespan = checker.calTimespan(calendar.getTime(), minutesBefore);

		assertEquals(sdf.format(calendar.getTime()), sdf.format(timespan.getStartHour()));

		assertEquals(5, timespan.getMinutes().size());

		for (int i = 1; i <= minutesBefore; i++) {
			assertTrue(timespan.getMinutes().contains(currentMinute - i));
		}

	}

}
