package edu.pc3.sensoract.vpds.data;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import edu.pc3.sensoract.vpds.api.DataQueryV2;
import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.ucla.nesl.sensorsafe.db.informix.InformixDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat.Datapoint;

public class DataArchiever {

	public static boolean enableIfx = false;
	
	static {		
		if(enableIfx) {
			try {			
				System.out.println("connecting to Ifx.....");
				InformixDatabaseDriver.initializeConnectionPool();
				//InformixUserDatabaseDriver.initializeDatabase();
				InformixStreamDatabaseDriver.initializeDatabase();
				System.out.println("done...");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static int stream_id = 10;	
	public static void ifxCreateDatastream(String uname, final String datastream, String ch) {
		
		List<Channel> chList = new ArrayList<Channel>();
		chList.add(new Channel(ch, "float"));
		Stream st = new Stream(stream_id, datastream, "tags", chList);
		
		try {
			InformixStreamDatabaseDriver ifx = new InformixStreamDatabaseDriver();			
			ifx.createStream(uname, st);
			stream_id++;
			ifx.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void ifxAddTuple(String username, String stream, String channel, String time, String value) {
		
		InformixStreamDatabaseDriver ifx = null;
		try {
		ifx = new InformixStreamDatabaseDriver();
			
			String d = "{\"timestamp\" : \"" + time + "\", \"tuple\" : [" + value + "]}"; 
			//String d = "[\"" + timeStamp.toString() + "\", " + i + "]";
			//System.out.println(d);
			ifx.addTuple(username, stream, d);
			ifx.close();
		} catch(Exception e) {
			System.out.println("Error add tuple : " + e.getMessage());
			if (e.getMessage().contains("does not exists")) {
				System.out.println("Creating datastream " + stream);
				ifxCreateDatastream(username, stream, channel);
			}
		} finally {			
			if(ifx != null) {
				try {
					ifx.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void storeDatapoint(String username, String device,
			String sensor, String channel, long timestamp, String value) {

		String datastreamName = DBDatapoint.getCollectionName(username, device,
				sensor, channel);		
		try {			
			DBDatapoint.save(username, device, sensor, channel, timestamp, value);			
			if(enableIfx) {
				Timestamp ts = new Timestamp(timestamp);
				//System.out.println("adding to ds " + datastreamName);
				ifxAddTuple(username, datastreamName, channel, ts.toString(), value);				
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error storeDatapoint " + e.getMessage());
		}
	}
	
	private static void cleanUpStoredInfo(InformixStreamDatabaseDriver ifx) throws SQLException {
		if (ifx.storedPstmt != null) { 
			ifx.storedPstmt.close();
		}
		ifx.storedResultSet = null;
		ifx.storedPstmt = null;
		ifx.storedStream = null;
	}
	
	// ditto of getNextJsonTuple
	public static QueryDataOutputFormat retrieveIfx(InformixStreamDatabaseDriver ifx, String username,
			String device, String sensor, String channel, long start, long end, String timeformat ) throws SQLException {
		
		QueryDataOutputFormat out = new QueryDataOutputFormat();
		
		out.device = device;
		out.sensor = sensor;
		out.channel = channel;

		
		if (ifx.storedResultSet == null) {
			cleanUpStoredInfo(ifx);
			return null;
		}

		if (ifx.storedResultSet.isClosed() || ifx.storedResultSet.isAfterLast()) {
			cleanUpStoredInfo(ifx);
			return null;
		}

		String t, v;
		if ("ISO8601".equalsIgnoreCase(timeformat)) {
			while(ifx.storedResultSet.next()) {	
				Timestamp ts = ifx.storedResultSet.getTimestamp(2);		
				//t = ts.toString();
				t = new DateTime(ts.getTime(), DateTimeZone.UTC).toString();				
				v = ifx.storedResultSet.getDouble(3)+"";
				out.datapoints.add(new Datapoint(t,v));
			}
		} else {
			while(ifx.storedResultSet.next()) {	
				Timestamp ts = ifx.storedResultSet.getTimestamp(2);								
				t = ts.getTime()+"";
				v = ifx.storedResultSet.getDouble(3)+"";
				out.datapoints.add(new Datapoint(t,v));
			}
		}
		return out;
	}
	
	
	public static QueryDataOutputFormat getFromIfx(String username,
			String device, String sensor, String channel, long start, long end,
			String timeformat)  {
		
		InformixStreamDatabaseDriver ifx = null;
		try {			
			 ifx = new InformixStreamDatabaseDriver();
			 
			 String datastreamName = DBDatapoint.getCollectionName(username, device,
					sensor, channel);
			 
			//System.out.println("getFromIfx " + start + " " + end + " " + datastreamName);
			Timestamp tsStart = new Timestamp(start);
			Timestamp tsEnd = new Timestamp(end);
			
			//ifx.prepareQuery(username, username, datastreamName,  null, null, null, 100,0);
			ifx.prepareQuery(username, username, datastreamName,  tsStart.toString(), tsEnd.toString(), null, 0,0);
			
			QueryDataOutputFormat out =  retrieveIfx(ifx, username, device, sensor, channel, start, end, timeformat);			
			
			return out;
		} catch (Exception e) {			
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(ifx != null) {
				try {
					ifx.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	private static QueryDataOutputFormat getFromMongo(String username,
			String device, String sensor, String channel, long start, long end,
			String timeformat) {		
		String secretkey = SensorActAPI.userProfile.getSecretkey(username);		
		if(secretkey == null) 
			return null;		
		return DataQueryV2.readDataNew(secretkey, device, sensor, channel, start, end, timeformat);		
	}
	
	public static QueryDataOutputFormat getDatapoints(String username,
			String device, String sensor, String channel, long start, long end,
			String timeformat ) {
		
		return getFromMongo(username, device, sensor, channel, start, end, timeformat);
		//return getFromIfx(username, device, sensor, channel, start, end, timeformat);
	}
	
}
