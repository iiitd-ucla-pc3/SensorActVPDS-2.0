/**
 * 
 */
package edu.pc3.sensoract.vpds.tasklet;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author samy
 * 
 */
public class DeviceId {

	private String username = null;
	private String device = null;
	private String sensor = null;
	private String channel = null;

	public DeviceId(String username, String device, String sensor, String id) {
		super();
		this.username = username;
		this.device = device;
		this.sensor = sensor;
		this.channel = id;
	}

	// taskletDeviceIdFormat in the format of username:device:sensor:channel
	public static DeviceId parseDeviceId(String taskletDeviceIdFormat) {

		String username = null;
		String device = null;
		String sensor = null;
		String channel = null;

		StringTokenizer tokenizer = new StringTokenizer(taskletDeviceIdFormat,":");

		try {
			username = tokenizer.nextToken();
			device = tokenizer.nextToken();
			sensor = tokenizer.nextToken();
			channel = tokenizer.nextToken();
		} catch (Exception e) {
			return null;
		}
		return new DeviceId(username, device, sensor, channel);
	}

	@Override
	public String toString() {
		return username + ":" + device + ":" + sensor + ":" + channel;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		DeviceId other = (DeviceId) obj;
		if (username.equals(other.username) && device.equals(other.device)
				&& sensor.equals(other.sensor)
				&& channel.equals(other.channel)) {
			return true;
		}
		return false;
	}

	public DeviceId() {
	}

}

