package org.apache.blur.agent.types;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
