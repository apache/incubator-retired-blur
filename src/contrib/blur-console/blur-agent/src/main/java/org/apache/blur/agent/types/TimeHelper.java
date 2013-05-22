package org.apache.blur.agent.types;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeHelper {
	public static Calendar now() {
		return getAdjustedTime(new Date().getTime());
	}

	public static Calendar getAdjustedTime(long time) {
		Calendar cal = Calendar.getInstance();
		TimeZone z = cal.getTimeZone();
		cal.add(Calendar.MILLISECOND, -(z.getOffset(time)));
		return cal;
	}
	
	public static Calendar getTimeAgo(int timeAgoInMS){
		Calendar timeAgo = Calendar.getInstance();
		timeAgo.setTimeInMillis(now().getTimeInMillis());
		timeAgo.add(Calendar.MILLISECOND, -timeAgoInMS);
		return timeAgo;
	}
}
