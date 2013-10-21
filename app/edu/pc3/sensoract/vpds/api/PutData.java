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
 * Name: DataUploadWaveSegment.java
 * Project: SensorAct-VPDS 
 * Version: 1.0
 * Date: 2012-04-14
 * Author: Pandarasamy Arjunan
 */
package edu.pc3.sensoract.vpds.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.pc3.sensoract.vpds.api.request.WaveSegmentFormat;
import edu.pc3.sensoract.vpds.constants.Const;
import edu.pc3.sensoract.vpds.enums.ErrorType;
import edu.pc3.sensoract.vpds.exceptions.InvalidJsonException;
import edu.pc3.sensoract.vpds.model.Datapoint;

/**
 * data/upload/wavesegment API: Uploads the wave segments sent by a device to
 * repository.
 * 
 * @author Pandarasamy Arjunan
 * @version 1.0
 */
public class PutData extends SensorActAPI {

	private static int WaveSegmentSize = 5;
	private static boolean isSendResponseEnabled = true;
	private static Logger uploadLog = Logger.getLogger("UploadLogger");

	private HashMap<String, ArrayList<WaveSegmentFormat>> hashmapWaveSegments = new HashMap<String, ArrayList<WaveSegmentFormat>>();

	public PutData() {
		super();
	}

	/**
	 * Sends error message to the callel.
	 * 
	 * @param errorType
	 *            ErrorType object contains the status code and message
	 * @param msg
	 *            Error message
	 */
	private void sendError(ErrorType errorType, String msg) {
		if (isSendResponseEnabled) {
			response.sendFailure(Const.API_DATA_UPLOAD_WAVESEGMENT, errorType,
					msg);
		} else {
			response.sendEmpty(); // to complete the request
		}
	}

	public void upsert1(long ts, String v, String d, String s, String c) {
		new Datapoint(ts, v, d, s, c).save();		
	}

	public void upsert(long ts, String v, String d, String s, String c) {

		Datapoint dp = Datapoint
				.find("epoch device sensor channel", ts, d, s, c).get();
		if (dp == null) {
			// System.out.println("not exist..." + ts + v);
			new Datapoint(ts, v, d, s, c).save();
		} else {
			dp.setValue(v);
			dp.save();
			System.out.println("exist..." + ts + v);
		}
	}

	public void upload(long ts, int v) {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					upsert1(ts, "" + v, "d" + d, "s" + s, "c" + c);
				}
			}
		}
	}

	public final void doProcess(final String waveSegmentJson) {

		try {
			WaveSegmentFormat ws = convertToRequestFormat(waveSegmentJson,
					WaveSegmentFormat.class);

			long time = new Date().getTime();

			// upsert(time, time, "d", "s", "c");
			// upsert(time, time+1, "d", "s", "c");

			/*
			 * Datapoint dp = new Datapoint(time, "v", "device", "sensor",
			 * "channel"); dp.save();
			 * 
			 * Datapoint dp1 = new Datapoint(time + 1, "v", "device", "sensor",
			 * "channel"); dp1.save();
			 * 
			 * System.out.println(dp.getId()); System.out.println(dp1.getId());
			 * 
			 * Datapoint dp2 = new Datapoint(time, "v2", "device", "sensor",
			 * "channel"); Datapoint old = dp2.get();
			 * 
			 * if (old != null) { old.ds().merge(dp2); }
			 * 
			 * System.out.println(dp2.get()); System.out.println(dp2.getId());
			 * 
			 * response.SendSuccess("upload..", "msg");
			 * 
			 * Datapoint.find("time device sensor channel", "");
			 */
			for (int i = 0; i < 100000; i++) {
				upload(time + i, i);
				// Datapoint dp = new Datapoint( time+i, ""+i, "device",
				// "sensor", "channel");
				// dp.save();
				if (i % 100 == 0) {
					System.out.println(new Date().toLocaleString()
							+ "  Stored# " + i);
				}
			}
			
			Datapoint dp = Datapoint
					.find("epoch device sensor channel", time, "0", "0", "0").get();
				
			
			//org.apache.commons.math3.stat.descriptive.DescriptiveStatistics x;
			
			// http://code.google.com/p/morphia/wiki/Query#Ignoring_Fields
			Datapoint e = Datapoint.ds().createQuery(Datapoint.class).retrievedFields(false, "_id", "device", "sensor", "channel" ).get();
			
			response.sendJSON(json.toJson(e));
			
			
		} catch (InvalidJsonException e) {
			sendError(ErrorType.INVALID_JSON, e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			sendError(ErrorType.SYSTEM_ERROR, e.getMessage());
		}

	}
}
