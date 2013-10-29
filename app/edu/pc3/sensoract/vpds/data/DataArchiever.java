package edu.pc3.sensoract.vpds.data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;

public class DataArchiever {

	/*
	public static StreamDatabaseDriver streamDb;
	static {
		streamDb = InformixStreamDatabaseDriver.getInstance();
		try {
			System.out.println("connecting to Ifx.....");
			streamDb.connect();
			System.out.println("connecting to Ifx..... Success");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	public static int stream_id = 10;
	public static void createDatastream(final String datastream, String ch) {
		List<Channel> chList = new ArrayList<Channel>();
		chList.add(new Channel(ch, "float"));
		Stream st = new Stream(stream_id, datastream, "tags", chList);
		try {
			streamDb.createStream(st);
			stream_id++;
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

*/

	public static void storeDatapoint(String username, String device,
			String sensor, String channel, long timestamp, String value) {

		String datastreamName = DBDatapoint.getCollectionName(username, device,
				sensor, channel);

		try {
			
			DBDatapoint.save(username, device, sensor, channel, timestamp, value);			
			Timestamp ts = new Timestamp(timestamp);

			// System.out.println("adding to ds " + datastreamName);
			//streamDb.addTuple(datastreamName, ts.toString(), value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			if (e.getMessage().contains("does not exists")) {
				System.out.println("Creating datastream " + datastreamName);
				//createDatastream(datastreamName, channel);
			}
		}
	}
}
