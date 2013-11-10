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
 * Name: DataQuery.java
 * Project: SensorAct-VPDS 
 * Version: 1.0
 * Date: 2012-04-14
 * Author: Pandarasamy Arjunan
 */
package edu.pc3.sensoract.vpds.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import play.Play;
import edu.pc3.sensoract.vpds.api.request.DataQueryFormat;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat.Datapoint;
import edu.pc3.sensoract.vpds.constants.Const;
import edu.pc3.sensoract.vpds.data.DataArchiever;
import edu.pc3.sensoract.vpds.enums.ErrorType;
import edu.pc3.sensoract.vpds.exceptions.InvalidJsonException;
import edu.pc3.sensoract.vpds.guardrule.GuardRuleManager;
import edu.pc3.sensoract.vpds.guardrule.RequestingUser;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.pc3.sensoract.vpds.model.WaveSegmentModel;
import edu.pc3.sensoract.vpds.util.SensorActLogger;

/**
 * data/query API: Retrieves wavesegmetns from the repository based upong the
 * given query.
 * 
 * @author Pandarasamy Arjunan
 * @version 1.0
 */
public class DataQueryV2 extends SensorActAPI {

	/**
	 * Validates the query attributes. If validation fails, sends corresponding
	 * failure message to the caller.
	 * 
	 * @param queryObj
	 *            Query in object format
	 */
	private void validateRequest() {
	}

	// modified data/query which pass through guard rule engine
	private void readData(final DataQueryFormat query) {

		String username = null;
		String ownername = null;
		String email = null;

		username = shareProfile.getUsername(query.secretkey);
		if (null == username) {
			ownername = userProfile.getUsername(query.secretkey);
			if (null == ownername) {
				response.sendFailure(Const.API_DATA_QUERY,
						ErrorType.UNREGISTERED_SECRETKEY, query.secretkey);
			}
		}

		// fetch the email address of the
		if (username != null) {
			email = shareProfile.getEmail(username);
			// update the ownername to fetch data
			ownername = userProfile.getOwnername();
		} else { // owner
			email = userProfile.getEmail(ownername);
		}

		RequestingUser requestingUser = new RequestingUser(email);

		long tStart = new Date().getTime();

		List<WaveSegmentModel> wsList = GuardRuleManager.read(ownername,
				requestingUser, query.devicename, query.sensorname,
				query.sensorid, query.conditions.fromtime,
				query.conditions.totime);
		long tEnd = new Date().getTime();

		SensorActLogger.info("Data size for " + query.devicename + ":"
				+ query.sensorname + " is " + wsList.size());
		SensorActLogger.info("With Guardrules:: Time to retrieve data: "
				+ (tEnd - tStart) / 1000 + " seconds");

		long t1 = new Date().getTime();
		// TODO: what the hell is happening here ?? Need to change the output
		// format
		Iterator<WaveSegmentModel> iteratorData = wsList.iterator();
		ArrayList<String> outList = new ArrayList<String>();

		while (iteratorData.hasNext()) {

			WaveSegmentModel ww = iteratorData.next();
			ww.data.timestamp = ww.data.timestamp; // for plot

			// ww.data.channels.removeAll(Collections.singleton(null));;
			// ww.data.channels.removeAll(Arrays.asList(new Object[]{null}));
			String data = json.toJson(ww);
			outList.add(data);
		}

		long t2 = new Date().getTime();
		SensorActLogger.info("Iterator Array Addition: " + (t2 - t1) / 1000
				+ " seconds\n\n");
		
		// response.SendJSON(of);
		// System.out.println(outList.toString());
		renderText("{\"wavesegmentArray\":" + outList.toString() + "}");
	}
	
	// modified data/query which pass through guard rule engine
	public static QueryDataOutputFormat readDataNew(String secretkey,
			String device, String sensor, String channel, long start, long end,
			String timeformat) {

		try {

			String username = null;
			username = userProfile.getUsername(secretkey);
			if (null == username) {
				response.sendFailure(Const.API_DATA_QUERY,
						ErrorType.UNREGISTERED_SECRETKEY, secretkey);
			}

			if ((start + "").length() == 10) {
				start = start * 1000;
			}

			if ((end + "").length() == 10) {
				end = end * 1000;
			}
			
			System.out.println("Query " + start + "  " + end + " "+ DBDatapoint.getCollectionName(username, device,sensor, channel) );

			// TODO: get data through guard rule engine
			List<DBDatapoint> dataPoints = DBDatapoint.fetch(username, device,
					sensor, channel, start, end);

			// String datastreamName = DBDatapoint.getCollectionName(username,
			// query.devicename,
			// query.sensorname, query.channelname);

			QueryDataOutputFormat out = new QueryDataOutputFormat();

			out.device = device;
			out.sensor = sensor;
			out.channel = channel;

			String time = null;
			if ("ISO8601".equalsIgnoreCase(timeformat)) {
				for (DBDatapoint dp : dataPoints) {
					time = new DateTime(dp.epoch, DateTimeZone.UTC).toString();
					out.datapoints.add(new Datapoint(time, dp.value));
				}
			} else {
				for (DBDatapoint dp : dataPoints) {
					time = "" + dp.epoch;
					out.datapoints.add(new Datapoint(time, dp.value));
				}
			}
			return out;
			
		} catch (Exception e) {			
			e.printStackTrace();
			return null;
		}
	}

	// modified data/query which pass through guard rule engine
	public static QueryDataOutputFormat readDataIfx(String secretkey,
			String device, String sensor, String channel, long start, long end,
			String timeformat) {

		try {

			String username = null;
			username = userProfile.getUsername(secretkey);
			if (null == username) {
				response.sendFailure(Const.API_DATA_QUERY,
						ErrorType.UNREGISTERED_SECRETKEY, secretkey);
			}
			if ((start + "").length() == 10) {
				start = start * 1000;
			}

			if ((end + "").length() == 10) {
				end = end * 1000;
			}
			System.out.println("Query " + start + "  " + end + " "+ DBDatapoint.getCollectionName(username, device,sensor, channel) );
			return DataArchiever.getFromIfx(username, device, sensor, channel, start, end, timeformat);			
		} catch (Exception e) {			
			e.printStackTrace();
			return null;
		}
	}

	// private void sendData(List<WaveSegmentModel> allWaveSegments) {
	// }

	/**
	 * Services the querydata API. Retrieves data from the repository as per the
	 * request query and sends back to the caller.
	 * 
	 * @param queryJson
	 *            Request query in Json string
	 */
	public void doProcess(String secretkey, String device, String sensor,
			String channel, long start, long end, String timeformat) {

		try {
			validateRequest();
			QueryDataOutputFormat out = readDataIfx(secretkey, device, sensor,
					channel, start, end, timeformat);
			renderJSON(out);
		} catch (Exception e) {
			SensorActLogger.error("Error:" + e.getMessage());
			e.printStackTrace();
			response.sendFailure(Const.API_DATA_QUERY, ErrorType.SYSTEM_ERROR,
					e.getMessage());
		}
	}

}
