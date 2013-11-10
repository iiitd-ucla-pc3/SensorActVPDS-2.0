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

import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import play.Play;
import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.libs.WS.WSRequest;
import edu.pc3.sensoract.vpds.api.request.WaveSegmentFormat;
import edu.pc3.sensoract.vpds.constants.Const;
import edu.pc3.sensoract.vpds.data.DataArchiever;
import edu.pc3.sensoract.vpds.enums.ErrorType;
import edu.pc3.sensoract.vpds.exceptions.InvalidJsonException;

/**
 * Device data upload API: Uploads channel readings sent by a device to
 * repository.
 * 
 * @author Pandarasamy Arjunan
 * @version 1.0
 */
public class DataUpload extends SensorActAPI {

	private static boolean isSendResponseEnabled = true;
	
	public static final Logger LOG = Logger.getLogger(DataUpload.class.getName());
	
	static {
		    for (Enumeration appenders=LOG.getAllAppenders(); appenders.hasMoreElements(); )  {
		        Appender appender = (Appender) appenders.nextElement();
		        System.out.println(appender.getName());
		    }
	}
	
	public DataUpload() {
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

	private void validateWaveSegmentData(final WaveSegmentFormat waveSegment) {

		WaveSegmentFormat.DeviceData data = waveSegment.data;

		if (null == data) {
			validator.addError(Const.PARAM_WS_DATA, Const.PARAM_WS_DATA
					+ Const.MSG_REQUIRED);
			return;
		}

		validator.validateWaveSegmentDeviceName(data.dname);
		validator.validateWaveSegmentSensorName(data.sname);
		validator.validateWaveSegmentSensorId(data.sid);
		//validator.validateWaveSegmentSInterval(data.sinterval);
		validator.validateWaveSegmentTimestamp(data.timestamp);

		if (null == data.channels || data.channels.isEmpty()) {
			validator.addError(Const.PARAM_WS_CHANNELS, Const.PARAM_WS_DATA
					+ "." + Const.PARAM_WS_CHANNELS + Const.MSG_REQUIRED);
			return;
		}

		for (int cIndex = 0; cIndex < data.channels.size(); ++cIndex) {
			WaveSegmentFormat.Channels channel = data.channels.get(cIndex);
			validator.validateWaveSegmentChannelName(channel.cname, cIndex);
			validator.validateWaveSegmentChannelUnit(channel.unit, cIndex);

			if (null == channel.readings || channel.readings.isEmpty()) {
				validator.addError(Const.PARAM_WS_READINGS, Const.PARAM_WS_DATA
						+ "." + Const.PARAM_WS_CHANNELS + "[" + cIndex + "]."
						+ Const.PARAM_WS_READINGS + Const.MSG_REQUIRED);
				return;
			}

			// TODO: check the type of data and any error in each readings
		}
	}

	/**
	 * Validates the wave segment attributes. If validation fails, sends
	 * corresponding failure message, if enabled, to the caller.
	 * 
	 * @param waveSegment
	 *            Wave segment of a sensor sent by a device
	 */
	private void validateWaveSegment(final WaveSegmentFormat waveSegment) {

		// TODO: Add validation for other attributes as well.
		validator.validateSecretKey(waveSegment.secretkey);
		validateWaveSegmentData(waveSegment);

		if (validator.hasErrors()) {
			sendError(ErrorType.VALIDATION_FAILED, validator.getErrorMessages());
		}
	}

	private void verifyWaveSegment(final WaveSegmentFormat waveSegment) {
		
		String secretkey = Play.configuration
				.getProperty(Const.OWNER_OWNERKEY);

		//System.out.println("owner key " + secretkey);
		if (!secretkey.equals(waveSegment.secretkey)) {
		//	response.sendFailure(Const.API_DATA_UPLOAD_WAVESEGMENT,
			//		ErrorType.UNREGISTERED_SECRETKEY, waveSegment.secretkey);

		}
		// TODO: verifty the device parameters
	}
	
	public void sendtoAnother(final WaveSegmentFormat waveSegment) {		
		try {
			String url="http://128.97.93.31:9000/data/upload/wavesegment";
			String ws = json.toJson(waveSegment);		
			WSRequest wsr = WS.url(url).body(ws).timeout("10min");		
			HttpResponse trainRes = wsr.post();			
		} catch (Exception e) {
			LOG.info("sendtoAnother.. " + e.getMessage());
		}
	}

	private static void validateRequest(String secretkey, String device, String sensor,
			String channel, String timestamp, String value) {
		
		String username = null;
		if (userProfile.isRegisteredSecretkey(secretkey)) {
			username = userProfile.getUsername(secretkey);
		} else {
				response.sendFailure(Const.API_DATA_UPLOAD_WAVESEGMENT,
					ErrorType.UNREGISTERED_SECRETKEY, secretkey);				
		}
		
		//TODO: other validations

	}

	public final void storeDataPoint(String username, String device, String sensor,
			String channel, long time, String value) {
		
		// convert into millis
		if((time+"").length() == 10) {
			time = time * 1000;
		}		
		DataArchiever.storeDatapoint(username, device, sensor, channel, time, value);
		deviceEvent.notifyDataArrived(username, device, sensor, channel, time, value);

	}
		
	/**
	 * Store the wave segment to the repository.
	 * 
	 * @param waveSegment
	 *            Wave segment of a sensor sent by a device
	 */
	public void handleWaveSegment(final WaveSegmentFormat waveSegment) {

		//sendtoAnother(waveSegment);
		
		String username = null;
		if (userProfile.isRegisteredSecretkey(waveSegment.secretkey)) {
			username = userProfile.getUsername(waveSegment.secretkey);
		} else {
			response.sendFailure(Const.API_DATA_UPLOAD_WAVESEGMENT,
					ErrorType.UNREGISTERED_SECRETKEY, waveSegment.secretkey);				
		}
		
		if(waveSegment.data.dname.contains("Motion") || waveSegment.data.dname.contains("Door")) {
			;
		} else {
			return;
		}
		
		String device = waveSegment.data.dname;
		String sensor = waveSegment.data.sname;
						
		//long timestamp = waveSegment.data.timestamp;
		long timestamp = DateTime.now().getMillis();
		
		// TODO: handle ISO8601 timestamp
		//System.out.println(timestamp);		
		if((timestamp+"").length() == 10) {
			timestamp = timestamp * 1000;
		}		
		//System.out.println(timestamp);
		
		for(WaveSegmentFormat.Channels ch : waveSegment.data.channels) {			
			for(Double d : ch.readings) {
				// for informix
				//String data = d.toString();
				//if(data.length() > 10) {
					//data = data.substring(0,10);
				//}				
				storeDataPoint(username, device, sensor, ch.cname, timestamp, d.toString());
			}
		}
		
		// System.out.println(System.currentTimeMillis()/1000 + " "
		// + waveSegment.data.sid + " notifing... " +
		// waveSegment.data.timestamp);
		//deviceEvent.notifyWaveSegmentArrived(waveSegment);
		// System.out.println(System.currentTimeMillis()/1000 + " "
		// + waveSegment.data.sid + " notified...");

	}

		
	/**
	 * Services the upload/wavesegment API. Received sensor readings in wave
	 * segment is persisted in the repository.
	 * 
	 * @param waveSegmentJson
	 *            Wave segment of a sensor sent by a device in json string
	 */
	public final void doProcess(final String waveSegmentJson) {

		try {
			WaveSegmentFormat newWaveSegment = convertToRequestFormat(
					waveSegmentJson, WaveSegmentFormat.class);

			validateWaveSegment(newWaveSegment);
			verifyWaveSegment(newWaveSegment);
			handleWaveSegment(newWaveSegment);
			
		} catch (InvalidJsonException e) {
			sendError(ErrorType.INVALID_JSON, e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			sendError(ErrorType.SYSTEM_ERROR, e.getMessage());
		}
	}
	/**
	 * Services the /devcie/sensor/channel/timestamp/value. Received sensor readings in wave
	 * segment is persisted in the repository.
	 * 
	 */
	public final void doProcess(String secretkey, String device, String sensor,
			String channel, String timestamp, String value) {
		try {
			
			//TODO
			//validateRequest(secretkey, device, sensor, channel, timestamp, value);
		
			// bypass key validataion
		 secretkey = Play.configuration
					.getProperty(Const.OWNER_OWNERKEY);
			
			String username = null;
			if (userProfile.isRegisteredSecretkey(secretkey)) {
				username = userProfile.getUsername(secretkey);
			} else {
				response.sendFailure(Const.API_DATA_UPLOAD_WAVESEGMENT,
						ErrorType.UNREGISTERED_SECRETKEY, secretkey);				
			}
			
			long time = Long.parseLong(timestamp);
			
			
			storeDataPoint(username, device, sensor, channel, time, value);
			
			//deviceEvent.notifyWaveSegmentArrived(waveSegment);

		}catch (Exception e) {
			e.printStackTrace();
			sendError(ErrorType.SYSTEM_ERROR, e.getMessage());
		}
	}

}
