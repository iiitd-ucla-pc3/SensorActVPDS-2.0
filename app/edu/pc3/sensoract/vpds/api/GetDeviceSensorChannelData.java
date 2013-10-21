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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Observer;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.ejb.criteria.expression.function.AggregationFunction.SUM;
import org.joda.time.DateTime;

import com.google.gson.GsonBuilder;
import com.mongodb.Mongo;

import play.Play;
import play.data.validation.Error;
import play.db.DB;
import play.db.jpa.JPA;
import edu.pc3.sensoract.vpds.api.request.WaveSegmentFormat;
import edu.pc3.sensoract.vpds.constants.Const;
import edu.pc3.sensoract.vpds.enums.ErrorType;
import edu.pc3.sensoract.vpds.exceptions.InvalidJsonException;
import edu.pc3.sensoract.vpds.model.Datapoint;
import edu.pc3.sensoract.vpds.model.WaveSegmentModel;
import edu.pc3.sensoract.vpds.tasklet.DeviceEvent;
import edu.pc3.sensoract.vpds.tasklet.DeviceEventListener;
import edu.pc3.sensoract.vpds.util.SensorActLogger;

/**
 * data/upload/wavesegment API: Uploads the wave segments sent by a device to
 * repository.
 * 
 * @author Pandarasamy Arjunan
 * @version 1.0
 */
public class GetDeviceSensorChannelData extends SensorActAPI {

	private String device;
	private String sensor;
	private String channel;

	private long start;
	private long end;
	private long interval;

	private String[] functions = null;

	private List<String> validFunctions;

	private static final String SUM = "SUM";
	private static final String COUNT = "COUNT";
	private static final String AVERAGE = "AVERAGE";
	private static final String MEDIAN = "MEDIAN";
	private static final String MINIMUM = "MINIMUM";
	private static final String MAXIMUM = "MAXIMUM";

	public GetDeviceSensorChannelData() {
		super();

		validFunctions = new ArrayList<String>();
		validFunctions.add("SUM");
		validFunctions.add("COUNT");
		validFunctions.add("AVERAGE");
		validFunctions.add("MEDIAN");
		validFunctions.add("MINIMUM");
		validFunctions.add("MAXIMUM");
	}

	// time in millis since EPOCH
	public void validateLimits(long start, long end, long interval) {

	}

	public void parseFunctions(String functions) {

		if (functions != null && functions.length() > 0) {
			this.functions = functions.split(",");

			for (String fun : this.functions) {
				if (!validFunctions.contains(fun.toUpperCase())) {
					validation.addError("function", "Invalid function " + fun);
				}
				fun = fun.toUpperCase();
			}
		}
	}

	public void parseRequest(String device, String sensor, String channel,
			String start, String end, String interval, String functions) {

		this.device = device;
		this.sensor = sensor;
		this.channel = channel;

		parseFunctions(functions);

		validation.required(start).message("from" + Const.MSG_REQUIRED);
		validation.required(end).message("to" + Const.MSG_REQUIRED);

		// TODO: validate from and to times

		try {
			this.start = Long.parseLong(start);
			this.end = Long.parseLong(end);
		} catch (Exception e) {
			validation.addError("from/to", "Invalid from/to time", start, end);
		}

		try {
			if (interval != null) {
				this.interval = Long.parseLong(interval);
			}
		} catch (Exception e) {
			validation.addError("interval", "Invalid inveral", interval);
		}

		if (validation.hasErrors()) {
			response.sendFailure("GET /device/...",
					ErrorType.VALIDATION_FAILED, validator.getErrorMessages());
		}
	}

	private Datapoint applyFunction(List<Datapoint> dataPointList) {

		if (functions == null || functions.length == 0) {
			// get the last item
			return dataPointList.get(dataPointList.size() - 1);
		}

		DescriptiveStatistics stat = new DescriptiveStatistics();
		// stat.addValue(0);

		double value;
		for (Datapoint dp : dataPointList) {
			value = Double.parseDouble(dp.getValue());
			stat.addValue(value);
		}
		
		// get the last data point
		Datapoint newDataPoint = dataPointList.get(dataPointList.size() - 1);
		newDataPoint.setValue(null);
		
		for (String function : functions) {
			switch (function.toUpperCase()) {
			case SUM:
				newDataPoint.sum = "" + stat.getSum();
				break;
			case COUNT:
				newDataPoint.count = "" + stat.getN();
				break;
			case AVERAGE:
				newDataPoint.average = "" + stat.getMean();
				break;
			case MEDIAN:
				// newDataPoint.median = ""+stat.get
				break;
			case MINIMUM:
				newDataPoint.minimum = "" + stat.getMin();
				break;
			case MAXIMUM:
				newDataPoint.maximum = "" + stat.getMax();
				break;
			}
		}

		return newDataPoint;
	}

	// Assumption : interval should be multiples of 60seconds or multiples of
	// 10s if less than 60
	/**
	 * 
	 * @param device
	 * @param sensor
	 * @param channel
	 * @param start
	 *            in milli-seconds
	 * @param end
	 *            in milli-seconds
	 * @param interval
	 * @param function
	 * @return
	 */
	private List<Datapoint> queryData() {

		System.out.println("Device " + device + " Sensor " + sensor
				+ " Channel " + channel + " start " + start + " end " + end
				+ "  interval " + interval + "  function " + functions);

		List<Datapoint> outList = new ArrayList<Datapoint>();

		if (0 == interval) {
			outList = Datapoint.fetchData(device, sensor, channel, start, end);
			System.out.print("Fetching.. " + device + ":" + sensor + ":"
					+ channel + " " + start + " .. " + end + "  .."
					+ outList.size() + " total points");
			return outList;
		}

		// start = 12213;
		// end = 58213;
		// interval = 10;

		long start1 = start / 1000;
		long end1 = end / 1000;

		System.out.println(start + " " + end + "  " + start1 + " " + end1);

		long start2 = start1 - (start1 % interval);
		long end2 = end1 - (end1 % interval);
		System.out.println(start + " " + end + "  " + start2 + " " + end2);

		long start3, end3;

		for (long time = start2; time < end2; time += interval) {

			// convert into millis
			start3 = time * 1000 + 1;
			end3 = (time + interval) * 1000;

			System.out.print("Fetching.. " + device + ":" + sensor + ":"
					+ channel + " " + start3 + " .. " + end3);

			List<Datapoint> dataPointList1 = Datapoint.fetchData(device,
					sensor, channel, start3, end3);

			if (dataPointList1 != null && !dataPointList1.isEmpty()) {
				System.out.println("  .." + dataPointList1.size()
						+ " data Points");
			} else {
				System.out.println("  .." + 0 + " data Points");
			}
			// continue, if no data points found
			if (dataPointList1 != null && !dataPointList1.isEmpty()) {
				Datapoint dataPoint1 = applyFunction(dataPointList1);
				outList.add(dataPoint1);
			}
		}

		System.out.println("Fetching.. " + device + ":" + sensor + ":"
				+ channel + " " + start + " .. " + end + "  .."
				+ outList.size() + " total points");

		return outList;
	}

	public final void doProcess(String device, String sensor, String channel,
			String start, String end, String interval, String functions) {

		try {

			System.out.println("Device " + device + " Sensor " + sensor
					+ " Channel " + channel + " from " + start + " to " + end);

			parseRequest(device, sensor, channel, start, end, interval,
					functions);

			Date d1 = new Date();
			System.out.println(d1 + " " + d1.getTime());

			List<Datapoint> outList = queryData();

			Date d2 = new Date();
			System.out.println(d2 + " " + d2.getTime() + "  "
					+ (d2.getTime() - d1.getTime()));

			String s = json.toJson(outList);

			Date d3 = new Date();
			System.out.println(d3 + " " + d3.getTime() + "  "
					+ (d3.getTime() - d2.getTime()));

			response.sendJSON(s);

			// Datapoint dp = Datapoint.find("epoch device sensor channel",
			// start,
			// device, sensor, channel).get();

			// org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
			// x;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
