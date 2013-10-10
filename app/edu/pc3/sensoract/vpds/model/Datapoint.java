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
 * Name: WaveSegmentChannelModel.java
 * Project: SensorAct-VPDS
 * Version: 1.0
 * Date: 2012-04-14
 * Author: Pandarasamy Arjunan
 */
package edu.pc3.sensoract.vpds.model;

import java.util.ArrayList;
import java.util.List;

import play.modules.morphia.Model;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.EntityListeners;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Index;
import com.google.code.morphia.annotations.Indexes;
import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.Transient;
import com.google.gson.annotations.SerializedName;

import play.modules.morphia.Model.Column;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Model class for wavesegment (channel)
 * 
 * @author Pandarasamy Arjunan
 * @version 1.0
 */
@Entity(value = "datapoint", noClassnameStored = true)
@Indexes(@Index("device, sensor, channel, -epoch"))
public class Datapoint extends Model {

	@com.google.code.morphia.annotations.Property("d")
	String device;

	@com.google.code.morphia.annotations.Property("s")
	String sensor;

	@com.google.code.morphia.annotations.Property("c")
	String channel;

	@com.google.code.morphia.annotations.Property("t")
	long epoch;

	@com.google.code.morphia.annotations.Property("v")
	String value;

	// exclude these attributes for persistence
	// @SerializedName("time")
	@Transient
	public DateTime time;

	@Transient
	public String count;

	@Transient
	public String sum;

	@Transient
	public String average;

	@Transient
	public String median;

	@Transient
	public String minimum;

	@Transient
	public String maximum;

	public Datapoint(long epoch, String value, String device, String sensor,
			String channel) {
		this.epoch = epoch;
		this.value = value;
		this.device = device;
		this.sensor = sensor;
		this.channel = channel;
	}

	public Datapoint setValue(String v) {
		this.value = v;
		return this;
	}

	public String getValue() {
		return this.value;
	}

	/**
	 * @see http://code.google.com/p/morphia/wiki/LifecycleMethods *
	 */
	@PrePersist
	void prePersist() {
	}

	@PostPersist
	void postPersist() {
	}

	@PreLoad
	void preLoad() {
	}

	@PostLoad
	void postLoad() {
		this.time = new DateTime(epoch, DateTimeZone.UTC);
	}

	// http://code.google.com/p/morphia/wiki/Query#Ignoring_Fields
	public static List<Datapoint> fetchData(String device, String sensor,
			String channel, long start, long end) {

		List<Datapoint> dataPoints = Datapoint.ds()
				.createQuery(Datapoint.class).filter("device", device)
				.filter("sensor", sensor).filter("channel", channel)
				.filter("epoch >=", start).filter("epoch <=", end)
				.retrievedFields(false, "_id", "device", "sensor", "channel")
				.order("epoch").asList();
		return dataPoints;
	}

}