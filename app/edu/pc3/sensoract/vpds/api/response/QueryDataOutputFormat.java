package edu.pc3.sensoract.vpds.api.response;

import java.util.ArrayList;
import java.util.List;

public class QueryDataOutputFormat {

	public static class Datapoint {
		
		public String time;
		public String value;
		
		public Datapoint(String t, String v) {
			time = t;
			value = v;
		}
	}
	
	public String device;
	public String sensor;
	public String channel;
	public List<Datapoint> datapoints = new ArrayList<Datapoint>();
}
