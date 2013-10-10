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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;

import edu.pc3.sensoract.vpds.api.SensorActAPI;

public class DBDatapoint {

	// private static final String COLLECTION_NAME = "datapoint";
	// private static DBCollection collection;

	public String value;
	public long epoch;

	public DateTime time;
	public String count;
	public String sum;
	public String average;
	public String median;
	public String minimum;
	public String maximum;

	private DB getDB() {

		Mongo mongo = null;
		try {
			mongo = new Mongo("localhost", 27017);
		} catch (UnknownHostException | MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Get the database object
		return mongo.getDB("sensoract");
	}

	private static DBCollection getCollection(String device, String sensor,
			String channel) {
		String name = device.concat("_").concat(sensor).concat("_")
				.concat(channel);

		// TODO: ??
		return Datapoint.db().getCollection(name);
	}

	public DBDatapoint() {
	}

	private DBDatapoint(long epoch, String value) {
		this.epoch = epoch;
		this.value = value;
	}

	private static DBObject getQuery(long start, long end) {
		return new QueryBuilder().put("epoch").greaterThanEquals(start)
				.lessThanEquals(end).get();
	}

	private static DBDatapoint toDBDatapoint(DBObject obj) {

		long epoch;
		String value;

		Object o = obj.get("epoch");
		if (o != null) {
			epoch = Long.parseLong(o.toString());
		} else
			return null;

		o = obj.get("value");
		if (o != null) {
			value = o.toString();
		} else
			return null;

		// DBDatapoint datapoint = new DBDatapoint(epoch, value+"ddd");
		// datapoint.time = new DateTime(datapoint.epoch, DateTimeZone.UTC);
		// datapoint.count = "d1111";

		DBDatapoint datapoint = new DBDatapoint();
		datapoint.epoch = epoch;
		datapoint.value = value;
		datapoint.time = new DateTime(datapoint.epoch, DateTimeZone.UTC);

		return datapoint;
	}

	private static DBObject getDBObject(long epoch, String value) {
		return BasicDBObjectBuilder.start().add("epoch", epoch)
				.add("value", value).get();
	}

	public static boolean save(String device, String sensor, String channel,
			long epoch, String value) {
		DBCollection col = getCollection(device, sensor, channel);
		col.save(getDBObject(epoch, value));
		return true;
	}

	public static long count(String device, String sensor, String channel) {
		DBCollection col = getCollection(device, sensor, channel);
		return col.count();
	}

	public static boolean drop(String device, String sensor, String channel) {
		DBCollection col = getCollection(device, sensor, channel);
		col.drop();
		return true;
	}

	public static boolean createIndex(String device, String sensor,
			String channel) {
		DBCollection col = getCollection(device, sensor, channel);
		col.createIndex(new BasicDBObject("epoch", -1));
		return true;
	}

	public static boolean reIndex(String device, String sensor, String channel) {
		DBCollection col = getCollection(device, sensor, channel);
		col.ensureIndex(new BasicDBObject("epoch", -1));
		return true;
	}

	// http://code.google.com/p/morphia/wiki/Query#Ignoring_Fields
	public static List<DBDatapoint> fetch(String device, String sensor,
			String channel, long start, long end) {

		DBCollection collection = getCollection(device, sensor, channel);
		DBObject query = getQuery(start, end);

		// System.out.println(collection.getFullName());
		// System.out.println(query.toString());

		System.out.println("\n" + new Date() + "   Querying.. ");

		DBCursor cursor = collection.find(query);

		// System.out.println("count : " + cursor.count() + " " +
		// cursor.explain());

		List<DBDatapoint> listDP = new ArrayList<DBDatapoint>();
		List<DBObject> listObj = cursor.toArray();		
		System.out.println(new Date() + "   done " + listObj.size());
		
		for (DBObject dbo : listObj) {
			listDP.add(toDBDatapoint(dbo));
		}
		
		System.out.println(new Date() + "   Converted..." + listObj.size());
		
		return listDP;
	}

	public void push(long t, String v) {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					DBDatapoint.save("d" + d, "s" + s, "c" + c, t, v);
				}
			}
		}

	}

	public void count() {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					long cnt = DBDatapoint.count("d" + d, "s" + s, "c" + c);
					String col = DBDatapoint.getCollection("d" + d, "s" + s,
							"c" + c).getFullName();
					System.out.println(col + " " + cnt);
				}
			}
		}

	}

	public void fetch(long start, long end) {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					String col = DBDatapoint.getCollection("d" + d, "s" + s,
							"c" + c).getFullName();
					System.out.print(new Date() + " Fetching " + col);
					List<DBDatapoint> list = DBDatapoint.fetch("d" + d,
							"s" + s, "c" + c, start, end);
					System.out.println("   Fetchead # " + list.size());
				}
			}
		}

	}

	public void index() {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					String col = DBDatapoint.getCollection("d" + d, "s" + s,
							"c" + c).getFullName();
					System.out.println(new Date() + " Indexing " + col);
					DBDatapoint.createIndex("d" + d, "s" + s, "c" + c);
					DBDatapoint.reIndex("d" + d, "s" + s, "c" + c);
				}
			}
		}

	}

	public void drop() {
		for (int d = 0; d < 2; ++d) {
			for (int s = 0; s < 2; ++s) {
				for (int c = 0; c < 2; ++c) {
					DBDatapoint.drop("d" + d, "s" + s, "c" + c);
				}
			}
		}

	}

}