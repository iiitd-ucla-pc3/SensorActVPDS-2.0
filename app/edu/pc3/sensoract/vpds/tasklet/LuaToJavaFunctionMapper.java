/*
 * Copyright (c) 2012, Indraprastha Institute of Information Technology,
 * Delhi (IIIT-D) and The Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 * 3. Neither the names of the Indraprastha Institute of Information
 *    Technology, Delhi and the University of California nor the names
 *    of their contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE IIIT-D, THE REGENTS, AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE IIITD-D, THE REGENTS
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */
/*
 * Name: LuaToJavaFunctionMapper.java
 * Project: SensorAct-VPDS
 * Version: 1.0
 * Date: 2012-07-20
 * Author: Pandarasamy Arjunan
 */

package edu.pc3.sensoract.vpds.tasklet;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.quartz.JobExecutionContext;

import play.Play;
import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat;
import edu.pc3.sensoract.vpds.data.DataArchiever;
import edu.pc3.sensoract.vpds.guardrule.GuardRuleManager;
import edu.pc3.sensoract.vpds.guardrule.RequestingUser;
import edu.pc3.sensoract.vpds.model.WaveSegmentModel;
import edu.pc3.sensoract.vpds.util.Plot;
import edu.pc3.sensoract.vpds.util.SensorActLogger;

public class LuaToJavaFunctionMapper {

	//private static Logger __LOG = LoggerFactory
		//	.getLogger(LuaToJavaFunctionMapper.class);
	
	//public static final Logger LOG = Logger.getLogger("Tasklet");

	public static final Logger LOG = Logger.getLogger(LuaToJavaFunctionMapper.class.getName());
	
	private JobExecutionContext jobContext = null;
//	public static double currentValue = 0;

	public LuaToJavaFunctionMapper(JobExecutionContext context) {
		jobContext = context;
	}

	public double[] getDoubleArray(int size) {
		double arr[] = new double[size];
		return arr;
	}
	
	public String[] getArray(int size) {
		int arr[] = new int[size];

		String arrS[] = new String[size];

		for (int i = 0; i < arr.length; ++i) {
			arr[i] = i * i;
			arrS[i] = "@" + i * i;
		}

		return arrS;
	}

	public Map getMap(int size) {

		Map map = new HashMap();
		long now = new Date().getTime();
		for (int i = 0; i < size; ++i) {
			map.put(now + i, i);
		}

		return map;
	}

	public void notifyEmail(Email email) {
		// System.out.println("notifyEmail : "
		// + jobContext.getJobDetail().getKey().getName() + " "
		// + jobContext.getJobDetail().getJobDataMap().get("email") + " "
		// + new Date().getTime());

		//long t1 = new Date().getTime();
		// System.out.println("before notifyEmail..." + new Date().getTime());
		email.sendNow(jobContext);
		//long t2 = new Date().getTime();
		// System.out.print(" notifyEmail :" + (t2-t1));
		// System.out.println("after notifyEmail..." + new Date().getTime());
	}

	public void sendTestEmail(String msg) {

		Email email = new Email("pandarasamya@iiitd.ac.in", "Tasklet notification : "+msg, msg, null);
		long t1 = new Date().getTime();
		System.out.println("before notifyEmail..." + new Date().getTime());
		email.sendNow(jobContext);
		long t2 = new Date().getTime();
		System.out.print(" notifyEmail :" + (t2-t1));
		System.out.println("after notifyEmail..." + new Date().getTime());
	}
	
	Map<Long,Double> toMap(QueryDataOutputFormat data) {

		Map<Long,Double> map = new LinkedHashMap<Long,Double>();
		
		List<Double> list = new ArrayList<Double>(map.values());
		
		// always return an empty map
		if (null == data || null == data.datapoints)
			return map;
		
		for (QueryDataOutputFormat.Datapoint dp : data.datapoints) {
			map.put(Long.parseLong(dp.time), Double.parseDouble(dp.value));
		}
		
		return map;
	}
	
	class DeviceInfo {
		String username = null;
		String device = null;
		String sensor = null;		
		String channel = null;		
	}
	
	public DeviceInfo toDeviceInfo(String resource) {
		
		DeviceInfo dd = new DeviceInfo();
		
		StringTokenizer tokenizer = new StringTokenizer(resource, ":");

		try {
			dd.username = tokenizer.nextToken();
			dd.device = tokenizer.nextToken();
			dd.sensor = tokenizer.nextToken();
			dd.channel = tokenizer.nextToken();
		} catch (Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at toDeviceInfo " + e.fillInStackTrace() );
			return null;
		}
		
		return dd;
	}

	// read past nSeconds data
	public Map read(String resource, int nSeconds) {

		DeviceInfo device = toDeviceInfo(resource);
		
		//long timeNow = new Date().getTime();
		long timeNow = DateTime.now(DateTimeZone.UTC).getMillis();

		QueryDataOutputFormat data = 
				DataArchiever.getDatapoints(device.username, device.device, device.sensor, device.channel, 
				timeNow-nSeconds*1000, timeNow, null);
		
		return toMap(data);
	}

	public double compute(QueryDataOutputFormat data, String function) {

		if( data == null || data.datapoints == null || data.datapoints.isEmpty()) {
			return Double.NaN;
		}
		
		//handle time functions
		if("FIRST_TIME".equalsIgnoreCase(function)){
			return Double.parseDouble(data.datapoints.get(0).time);
		} else if("FIRST_VALUE".equalsIgnoreCase(function)){
			return Double.parseDouble(data.datapoints.get(0).value);
		} else if("LAST_TIME".equalsIgnoreCase(function)){
			String d = data.datapoints.get(data.datapoints.size()-1).time;
			return Double.parseDouble(d);
		} else if("LAST_VALUE".equalsIgnoreCase(function)){
			String d = data.datapoints.get(data.datapoints.size()-1).value;
			return Double.parseDouble(d);			
		}
		
		DescriptiveStatistics stat = new DescriptiveStatistics();		
		
		for (QueryDataOutputFormat.Datapoint dp : data.datapoints) {			
			stat.addValue(Double.parseDouble(dp.value));
		}
		
		if(stat.getN() == 0 ) {
			return Double.NaN;
		}
		
		if(function.equalsIgnoreCase("SUM")) {
			return stat.getSum();
		} else if(function.equalsIgnoreCase("MEAN")) {
			return stat.getMean();
		}else if(function.equalsIgnoreCase("MIN")) {
			return stat.getMin();
		}else if(function.equalsIgnoreCase("MAX")) {
			return stat.getMax();
		}else if(function.equalsIgnoreCase("COUNT")) {
			return stat.getN();
		} else {
			return Double.NaN;
		}
	}
	
	// for nesl veris meter
	// overwirte the channel information and uses channel<start to end> 
	// 
	public Double readAll(String resource, String channel, int start, 
			int end, int nSeconds, String function) {
		
		DeviceInfo device = toDeviceInfo(resource);

		try {
			
			//long timeNow = new Date().getTime();
			long timeNow = DateTime.now(DateTimeZone.UTC).getMillis();
			
			
			double agg = 0;
			String ch;
			
			while(start <= end) {				
				ch = channel + start;
				
				//System.out.println("Reading .. " + ch);				
				QueryDataOutputFormat data = DataArchiever.getDatapoints(device.username, device.device, 
						device.sensor, ch, 
						timeNow-nSeconds*1000, timeNow, null);
				
				double val = compute(data, function);
				agg += val;
				
				++start;
			}
			
			//System.out.println("Sending email.. ");
			//sendTestEmail("Average is " + (sum/count));			
			//System.out.println("Returning.. " + val);
			return agg;
			
		} catch(Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at readAll " + e.fillInStackTrace() );						
			e.printStackTrace();
			return Double.NaN;
		}		
	}

	public Double read(String resource, int nSeconds, String function) {
		
		DeviceInfo device = toDeviceInfo(resource);
		String secretkey = null;
		try {

			//long timeNow = new Date().getTime();
			long timeNow = DateTime.now(DateTimeZone.UTC).getMillis();
			
			QueryDataOutputFormat data = DataArchiever.getDatapoints(device.username, device.device, 
					device.sensor, device.channel, 
					timeNow-nSeconds*1000, timeNow, null);

			double val = compute(data, function);
			
			//System.out.println("Sending email.. ");
			//sendTestEmail("Average is " + (sum/count));			
			//System.out.println("Returning.. " + val);
			return val;
			
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at read " + e.fillInStackTrace() );						
			e.printStackTrace();
			return Double.NaN;
		}
	}

	public Map read(String resource, long startTime, long endTime) {
		
		DeviceInfo device = toDeviceInfo(resource);		
		
		try {
			QueryDataOutputFormat data = DataArchiever.getDatapoints(device.username, device.device, 
					device.sensor, device.channel, 
					startTime, endTime, null);

			return toMap(data);
			
			//System.out.println("Sending email.. ");
			//sendTestEmail("Average is " + (sum/count));			
			//System.out.println("Returning.. " + val);
			
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at read " + e.fillInStackTrace() );						
			e.printStackTrace();
			return null;
		}
	}

	public Double read(String resource, long startTime, long endTime, String function) {		
		DeviceInfo device = toDeviceInfo(resource);		
		try {
			QueryDataOutputFormat data = DataArchiever.getDatapoints(device.username, device.device, 
					device.sensor, device.channel, 
					startTime, endTime, null);

			double val = compute(data, function);
			
			//System.out.println("Sending email.. ");
			//sendTestEmail("Average is " + (sum/count));			
			//System.out.println("Returning.. " + val);
			return val;
			
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at read " + e.fillInStackTrace() );						
			e.printStackTrace();
			return Double.NaN;
		}
	}

	
	public String plot(String resource, int nSeconds) {
		return plot(resource, nSeconds, "");
	}
	
	public String plot(String resource, int nSeconds, String unit) {
		
		DeviceInfo device = toDeviceInfo(resource);
		String secretkey = null;		
		try {
				//System.out.println("after.......................");
				secretkey = SensorActAPI.userProfile.getSecretkey(device.username);
				
				if(secretkey == null) 
					return null;
			
			//long timeNow = new Date().getTime();
			long timeNow = DateTime.now(DateTimeZone.UTC).getMillis();
			
			QueryDataOutputFormat data = DataArchiever.getDatapoints(device.username, device.device, 
					device.sensor, device.channel, 
					timeNow-nSeconds*1000, timeNow, null);
			
			//System.out.println("Creating plot...");
			return Plot.createPlot(toMap(data), device.channel, unit);		
			//System.out.println("Sending email.. ");
			//sendTestEmail("Average is " + (sum/count));			
			//System.out.println("Returning.. " + val);
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at plot " + e.fillInStackTrace() );						
			e.printStackTrace();
			return null;
		}
	}
	
	public String getEmailList() {		
		try {
			String filename = Play.applicationPath.getPath() + "/conf/email.list.txt";
			
			File file =  new File(filename);			
			FileInputStream fis = new FileInputStream(file);
		    byte[] data = new byte[(int)file.length()];
		    fis.read(data);		    
		    String s = new String(data, "UTF-8");			
			return s;
		} catch(Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at getEmailList " + e.fillInStackTrace() );						
			e.printStackTrace();
		}
		return null;
	}

	public boolean email(String to, String subject, String msg) {
		return email(to, subject, msg, null);
	}
	
	public boolean email(String to, String subject, String msg, String attachment) {
		
		try{		
			if(to==null || to.length() == 0) {
				to = getEmailList();
			}			
			StringTokenizer tokenizer = new StringTokenizer(to, ", ");			
			while(tokenizer.hasMoreTokens()) {
				String toemail = tokenizer.nextToken();
				//LOG.info("Sending email to : " + toemail + " " + msg);
				Email email = new Email(toemail, subject, msg, attachment);
				email.sendNow(jobContext);					
			}						
		}catch(Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at email " + e.fillInStackTrace() );						
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	
	public String getSMSnos() {		
		try {
			String filename = Play.applicationPath.getPath() + "/conf/sms.nos.txt";
			
			File file =  new File(filename);			
			FileInputStream fis = new FileInputStream(file);
		    byte[] data = new byte[(int)file.length()];
		    fis.read(data);		    
		    String s = new String(data, "UTF-8");
			 
			return s;
		} catch(Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at getSMSnos " + e.fillInStackTrace() );						
			e.printStackTrace();						
		}
		return null;
	}
	
	// toList is comma seperated list of mobile numbeers
	public boolean sms(String toList, String msg) {
		
		try{			
			if(toList==null || toList.length() == 0) {
				toList = getSMSnos();
			}			
			StringTokenizer tokenizer = new StringTokenizer(toList, ", ");			
			while(tokenizer.hasMoreTokens()) {
				String to = tokenizer.nextToken();
				//LOG.info("Sending SMS to: " + to + " " + msg);
				String context = jobContext.getJobDetail().getKey().toString();				
				SMSGateway.sendSMS(context, to, msg);	
			}				
		}catch(Exception e) {
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at sms " + e.fillInStackTrace() );						
			e.printStackTrace();						
			return false;
		}		
		return true;
	}
	
	public String time2str(long epoch) {		
		DateTime dt = new DateTime(epoch);
		return dt.toString();
	}
	
	// t1 - t2
	public String timedif(long t1, long t2) {
		DateTime dt1 = new DateTime(t1);
		DateTime dt2 = new DateTime(t2);
		
		int hh = Hours.hoursBetween(dt2, dt1).getHours() % 24;
		int mm = Minutes.minutesBetween(dt2, dt1).getMinutes() % 60;

		String diff =  String.format("%02d Hours and %02d Minutes", hh, mm);
		
		return diff;
	}
	
	public boolean writeData(String resource, double data) {
		
		try {
			DeviceInfo device = toDeviceInfo(resource);
			//WaveSegmentFormat ws = makeWaveSegment(device, data);
			
			String secretkey = SensorActAPI.userProfile.getSecretkey(device.username);
			if(secretkey == null) 
				return false;

			//System.out.println("writing new wavesegment " + jsonStr);
			//SensorActAPI.dataUploadWaveseg.doProcess(jsonStr);
			
			long timeNow = DateTime.now(DateTimeZone.UTC).getMillis();
			
			//SensorActAPI.dataUpload.doProcess(secretkey, device.device, device.sensor, 
				//	device.channel, timeNow+"", data+"");
			
			DataArchiever.storeDatapoint(device.username, device.device, device.sensor, device.channel, timeNow, data+"");
			
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at writeData " + e.fillInStackTrace() );						
			e.printStackTrace();						
			return false;
		}		
		return true;
	}
	
	public boolean writeData(String resource, long time, double data) {		
		try {
			DeviceInfo device = toDeviceInfo(resource);
			//WaveSegmentFormat ws = makeWaveSegment(device, data);
			
			//SensorActAPI.dataUpload.doProcess(secretkey, device.device, device.sensor, 
				//	device.channel, timeNow+"", data+"");
			
			DataArchiever.storeDatapoint(device.username, device.device, device.sensor, device.channel, time, data+"");
			
		} catch(Exception e) {			
			String context = jobContext.getJobDetail().getKey().toString();
			LOG.error(context + " Error at writeData " + e.fillInStackTrace() );						
			e.printStackTrace();						
			return false;
		}
		
		return true;
	}


	/**
	 * Reads wave segments from 'fromTime' to current time
	 * 
	 * @author Manaswi Saha
	 * @param resource
	 *            fromTime toTime
	 */

	public Map readPastToNow(String resource, long fromTime, long toTime) {

		long t1 = new Date().getTime();

		SensorActLogger.info("readPasttoNow Lua: fromtime: " + fromTime
				+ " ToTime: " + toTime);

		String username = null;
		String devicename = null;
		String sensorname = null;
		String sensorid = null;
		// String channelname = null;

		StringTokenizer tokenizer = new StringTokenizer(resource, ":");

		try {
			username = tokenizer.nextToken();
			devicename = tokenizer.nextToken();
			sensorname = tokenizer.nextToken();
			sensorid = tokenizer.nextToken();
			// channelname = tokenizer.nextToken();
		} catch (Exception e) {
		}

		// TODO: update the username as ownername
		//username = Play.configuration.getProperty(Const.OWNER_NAME);
		username = SensorActAPI.userProfile.getOwnername();

		String email = SensorActAPI.userProfile.getEmail(username);

		RequestingUser requestingUser = new RequestingUser(email);

		long t2 = new Date().getTime();
		List<WaveSegmentModel> wsList = GuardRuleManager.read(username,
				requestingUser, devicename, sensorname, sensorid, fromTime,
				toTime);

		SensorActLogger.info("No of readings got:" + wsList.size());

		long t3 = new Date().getTime();

		SensorActLogger.info("GuardRuleManager.read: " + (t3 - t2) + " total: "
				+ (t3 - t1));

		if (null == wsList)
			return null;

		//return toMap(wsList);
		return null;

	}

	/**
	 * @author Manaswi Saha
	 * @param resource
	 * @param status
	 *            device status - ON/OFF
	 */

	public boolean write(String resource, double status) {

		// long t1 = new Date().getTime();

		String username = null;
		String devicename = null;
		String actuatorname = null;
		String actuatorid = null;

		StringTokenizer tokenizer = new StringTokenizer(resource, ":");

		try {
			username = tokenizer.nextToken();
			devicename = tokenizer.nextToken();
			actuatorname = tokenizer.nextToken();
			actuatorid = tokenizer.nextToken();
		} catch (Exception e) {
		}

		System.out.println("after tokenizing");
		// System.out.println("write resource " + resource);
		// System.out.println("Write Resource " + username + " " + devicename +
		// " "
		// + actuatorname + " " + actuatorid + "status:" + status);

		// TODO: update the username as ownername
		//username = Play.configuration.getProperty(Const.OWNER_NAME);
		username = SensorActAPI.userProfile.getOwnername();

		String email = SensorActAPI.userProfile.getEmail(username);

		RequestingUser requestingUser = new RequestingUser(email);

		System.out.println("after user " + email);

		// long t2 = new Date().getTime();
		if (GuardRuleManager.write(username, requestingUser, devicename,
				actuatorname, actuatorid, status))
			return true;
		else
			SensorActLogger
					.info("GuardRuleManager:write():: unsuccessful for status "
							+ status);
		// long t3 = new Date().getTime();

		// SensorActLogger.info("GuardRuleManager.write: " + (t3 - t2) +
		// " total: "+ (t3 - t1));

		return false;
	}
	
	public String cachePut(String key, String value) {
		String taskletId = jobContext.getJobDetail().getKey().toString();		
		return TaskletCache.put(taskletId, key, value);
	}
	
	public String cacheGet(String key) {
		String taskletId = jobContext.getJobDetail().getKey().toString();
		return TaskletCache.get(taskletId, key);
	}

	public String cacheRemove(String key) {
		String taskletId = jobContext.getJobDetail().getKey().toString();
		return TaskletCache.remove(taskletId, key);
	}

	public void cacheRemoveAll() {
		String taskletId = jobContext.getJobDetail().getKey().toString();
		TaskletCache.removePrefixAll(taskletId);
	}

}
