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
 * Name: Application.java
 * Project: SensorAct-VPDS
 * Version: 1.0
 * Date: 2012-04-14
 * Authors: Pandarasamy Arjunan, Haksoo Choi
 */
package controllers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import play.mvc.Before;
import play.mvc.Controller;
import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.api.request.TaskletAddFormat;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.pc3.sensoract.vpds.tasklet.Email;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;

/**
 * Application class, entry point for all APIs.
 * 
 * @author Pandarasamy Arjunan, Haksoo Choi and Manaswi Saha
 * @version 1.1
 */

public class Application extends Controller {

	@Before
	static void handleHeaders() {
		// System.out.println("key is : " +
		// request.headers.get("x-key").value());
	}

	public static void index() {
		Email email = new Email("pandarasamya@iiitd.ac.in", "Tasklet notification", "HELLO");
		long t1 = new Date().getTime();
		System.out.println("before notifyEmail..." + new Date().getTime());
		email.sendNow(null);
		long t2 = new Date().getTime();
		System.out.print(" notifyEmail :" + (t2-t1));
		System.out.println("after notifyEmail..." + new Date().getTime());
		renderText("Welcome to SensorAct!");
	}

	// Repo information
	public static void repoInfo() {
		SensorActAPI.repoInfo.doProcess();
	}

	// User profile management
	public static void userLogin() {
		SensorActAPI.userLogin.doProcess(request.params.get("body"));
	}

	public static void userRegister() {
		SensorActAPI.userRegister.doProcess(request.params.get("body"));
	}

	public static void userList() {
		SensorActAPI.userList.doProcess(request.params.get("body"));
	}

	// User key management
	public static void keyGenerate() {
		SensorActAPI.keyGenerate.doProcess(request.params.get("body"));
	}

	public static void keyDelete() {
		SensorActAPI.keyDelete.doProcess(request.params.get("body"));
	}

	public static void keyList() {
		SensorActAPI.keyList.doProcess(request.params.get("body"));
	}

	public static void keyEnable() {
		SensorActAPI.keyEnable.doProcess(request.params.get("body"));
	}

	public static void keyDisable() {
		SensorActAPI.keyDisable.doProcess(request.params.get("body"));
	}

	// Device profile management
	public static void deviceAdd() {
		SensorActAPI.deviceAdd.doProcess(request.params.get("body"));
	}

	public static void deviceDelete() {
		SensorActAPI.deviceDelete.doProcess(request.params.get("body"));
	}

	public static void deviceGet() {
		SensorActAPI.deviceGet.doProcess(request.params.get("body"));
	}

	public static void deviceList() {
		SensorActAPI.deviceList.doProcess(request.params.get("body"));
	}

	public static void deviceShare() {
		SensorActAPI.deviceShare.doProcess(request.params.get("body"));
	}

	public static void deviceSearch() {
		SensorActAPI.deviceSearch.doProcess(request.params.get("body"));
	}

	// Device template management
	public static void deviceTemplateAdd() {
		SensorActAPI.deviceTemplateAdd.doProcess(request.params.get("body"));
	}

	public static void deviceTemplateDelete() {
		SensorActAPI.deviceTemplateDelete.doProcess(request.params.get("body"));
	}

	public static void deviceTemplateGet() {
		SensorActAPI.deviceTemplateGet.doProcess(request.params.get("body"));
	}

	public static void deviceTemplateList() {
		SensorActAPI.deviceTemplateList.doProcess(request.params.get("body"));
	}

	public static void deviceTemplateGlobalList() {
		SensorActAPI.deviceTemplateGlobalList.doProcess(request.params
				.get("body"));
	}

	// Guard rule management
	public static void guardRuleAdd() {
		SensorActAPI.guardRuleAdd.doProcess(request.params.get("body"));
	}

	public static void guardRuleDelete() {
		SensorActAPI.guardRuleDelete.doProcess(request.params.get("body"));
	}

	public static void guardRuleGet() {
		SensorActAPI.guardRuleGet.doProcess(request.params.get("body"));
	}

	public static void guardRuleList() {
		SensorActAPI.guardRuleList.doProcess(request.params.get("body"));
	}

	public static void guardRuleAssociationAdd() {
		SensorActAPI.guardRuleAssociationAdd.doProcess(request.params
				.get("body"));
	}

	public static void guardRuleAssociationDelete() {
		SensorActAPI.guardRuleAssociationDelete.doProcess(request.params
				.get("body"));
	}

	public static void guardRuleAssociationGet() {
		SensorActAPI.guardRuleAssociationGet.doProcess(request.params
				.get("body"));
	}

	public static void guardRuleAssociationList() {
		SensorActAPI.guardRuleAssociationList.doProcess(request.params
				.get("body"));
	}

	// Task management
	public static void taskletAdd() {
		System.out.println(request.params.get("body"));
		SensorActAPI.taskletAdd.doProcess(request.params.get("body"));
	}

	public static void taskletDelete() {
		SensorActAPI.taskleteDelete.doProcess(request.params.get("body"));
	}

	public static void taskletGet() {
		SensorActAPI.taskletGet.doProcess(request.params.get("body"));
	}

	public static void taskletList() {
		SensorActAPI.taskletList.doProcess(request.params.get("body"));
	}

	public static void taskletExecute() {
		SensorActAPI.taskletExecute.doProcess(request.params.get("body"));
	}

	public static void taskletStatus() {
		SensorActAPI.taskletStatus.doProcess(request.params.get("body"));
	}

	public static void taskletCancel() {
		SensorActAPI.taskletCancel.doProcess(request.params.get("body"));
	}

	// Data management
	public static void dataQuery() {
		SensorActAPI.dataQuery.doProcess(request.params.get("body"));
	}

	public static void dataQueryV2(String device, String sensor, String channel, 
			long start, long end, String timeformat) {
		
		final String XAPIKEY = "x-apikey";
		
		for(String k : request.headers.keySet()) {
			System.out.println(request.headers.get(k).name);
		}
		
		String secretkey = null;
		if(request.headers.containsKey(XAPIKEY)) {
			secretkey =  request.headers.get(XAPIKEY).value();
		} else {
			renderText("Unregistered secretkey");
		}
		
		System.out.println("secretkey " + secretkey + " " + start + " " + end);
		SensorActAPI.dataQueryv2.doProcess(secretkey, device, sensor, channel, start, end, timeformat);
	}

	public static void dataUploadWaveSegment() {
		SensorActAPI.dataUpload.doProcess(request.params.get("body"));
	}

	public static void putDeviceData(String device) {
		renderText("Device " + device);
		// SensorActAPI.putData.doProcess(request.params.get("body"));
	}

	public static void putDeviceSensorData(String device, String sensor) {
		renderText("Device " + device + " Sensor " + sensor);
		// SensorActAPI.putData.doProcess(request.params.get("body"));
	}

	// TODO: refer play framework cookbook about router head
	public static void putDeviceSensorChannelData(String device, String sensor,
			String channel, String time, String value) {

		// SensorActAPI.putData.doProcess(request.params.get("body"));

		final String XAPIKEY = "x-apikey";		
		//for(String k : request.headers.keySet()) {
			//System.out.println(request.headers.get(k).name);
		//}
		
		String secretkey = null;
		if(request.headers.containsKey(XAPIKEY)) {
			secretkey =  request.headers.get(XAPIKEY).value();
		} else {
			renderText("Unregistered secretkey");
		}

		SensorActAPI.dataUpload.doProcess(secretkey, device, sensor, channel, time, value);

		
		renderText("Device " + device + " Sensor " + sensor + " Channel "
				+ channel + " time " + time + " value " + value);
	}

	// TODO: refer play framework cookbook about router head
	public static void getDeviceSensorChannelData(String device, String sensor,
			String channel, String from, String to) {

		String interval = request.params.get("interval");
		String function = request.params.get("function");

		SensorActAPI.getDeviceSensorChannelData.doProcess(device, sensor,
				channel, from, to, interval, function);

		renderText("Device " + device + " Sensor " + sensor + " Channel "
				+ channel + " from " + from + " to " + to + "  interval "
				+ interval + "  function " + function);

	}

	public static void deviceActuate() {
		System.out.println(request.params.get("body"));
		SensorActAPI.deviceActuate.doProcess(request.params.get("body"));
	}

	// For Listing Actuation Requests
	public static void deviceListActuationRequest() {
		SensorActAPI.deviceListActuationRequest.doProcess(request.params
				.get("body"));
	}

	// For Canceling Actuation Requests
	public static void deviceCancelActuationRequest() {
		SensorActAPI.deviceCancelActuationRequest.doProcess(request.params
				.get("body"));
	}

	private static StreamDatabaseDriver streamDb;
	
	static {
		streamDb = InformixStreamDatabaseDriver.getInstance();
		try {
			System.out.println("connecting to Ifx.....");
			streamDb.connect();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// For development test purpose.
	public static void test() throws Exception {		
		
		List<Channel> chList  = new ArrayList<Channel>();		
		chList.add(new Channel("ch1", "float"));

		Stream st = new Stream(1,"nesl_owner__Test_Device1__Temperature__channel1", "tags", chList);
		
		try {
			streamDb.createStream(st);
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
		
		renderText("ddd");
		
		try {
			Log.info(DateTime.now().toString() + " Inserting...");
			DateTime start = DateTime.now();
			
			long t1 = DateTime.now().getMillis();
			long t2 = DateTime.now().getMillis();
			long t3 = DateTime.now().getMillis();
			
			DateTime iStart = start; 
			
			int i=0;
			while(true) {				
				DateTime now = start.plusMillis(i++);				
				Timestamp timeStamp = new Timestamp(now.getMillis());				
				streamDb.addTuple("ds", timeStamp.toString(), i+"");				
				if(i%1000 == 0 ) {
					t1 = DateTime.now().getMillis();
					Log.info(DateTime.now().toString() + " Inserted " + i + " " + (t1-t3));
					iStart = now;
					//Log.info(DateTime.now().toString() + " Querying...");					
					String stt = new Timestamp(start.getMillis()).toString();
					String ent = new Timestamp(t1).toString();					
					t2 = DateTime.now().getMillis();
					int size = streamDb.queryStream("ds", stt, ent, null);
					t3 = DateTime.now().getMillis();
					Log.info(DateTime.now().toString() + " Queried  " + size + " " + (t3-t2)+"\n");
				}
				break;
			}
			
			
			//renderJSON(obj);
			
		} catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		//renderText("dd");
		
		long t = new Date().getTime();

		DBDatapoint dbp = new DBDatapoint();

		dbp.drop();
		
		for (int v = 0; v <10; ++v) {
			dbp.push(t, v + "");
			if(v%1000 == 0 ) {
				System.out.println(new Date()+ " pushed# " + v);
			}
		}
		
		//t = 1373511533116l;		
		System.out.println("..........counting..");
		dbp.count();
		
		System.out.println("..........fetching..");
		//dbp.fetch(t, t+1000000);
		
		System.out.println("..........indexing.");
		//dbp.index();

		System.out.println("..........fetching..");
		//dbp.fetch(t, t+1000000);

	}
	
	public static void computedSensor1() {		
		TaskletAddFormat t1 = new TaskletAddFormat();
		
		t1.taskletname = "computed1";
		t1.desc = "desc";
		
		Map<String,String> param = new HashMap<String,String>();
		param.put("c1", "nesl_owner:Test_Device1:Temperature:channel1");
		
		Map<String,String> input = new HashMap<String,String>();
		param.put("t1", "[0/5 * 0-23 * *]");

		String script = "print( 'reading ', d1); \n" 
						+ "dd = VPDS:read(d1) \n"
						+ "print('its value ', dd )";
		
		t1.param = param;
		t1.input = input;
		
		t1.when = "t1";
		t1.execute = "script";

	}
}
