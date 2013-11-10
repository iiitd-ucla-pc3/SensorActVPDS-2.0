/**
 * 
 */
package edu.pc3.sensoract.vpds.tasklet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

import org.apache.log4j.Logger;
import org.quartz.JobDetail;

import edu.pc3.sensoract.vpds.api.DataUpload;
import edu.pc3.sensoract.vpds.api.request.WaveSegmentFormat;

/**
 * @author samy
 * 
 */
public class DeviceEvent extends Observable {
	
	//TODO: make the map thread safe for all the methods
	// use java.util.concurrent.* classes

	private static Map<String, ArrayList<DeviceEventListener>> mapListeners = 
			new HashMap<String, ArrayList<DeviceEventListener>>();
	
	

	public DeviceEvent() {
	//	mapListeners = new HashMap<String, ArrayList<DeviceEventListener>>();
	}

	/**
	 * 
	 * @param ws
	 */
	/*
	public void notifyWaveSegmentArrived(WaveSegmentFormat ws) {

		DeviceId deviceId = new DeviceId(ws.secretkey, ws.data.dname,
				ws.data.sname, ws.data.sid);
		
		DataUpload.LOG.info(System.currentTimeMillis()/1000 + " :: notifyWaveSegmentArrived.. DeviceId "
				+ deviceId.toString());

		ArrayList<DeviceEventListener> listListener = mapListeners.get(deviceId
				.toString());
		if (null == listListener)
			return;

		DataUpload.LOG.info("notifyWaveSegmentArrived.. Listeners "
				+ listListener.size() + "\n");

		for (DeviceEventListener listener : listListener) {
			listener.deviceDataReceived(ws);
		}
	}
	*/

	// TODO: added new format of data notification
	public void notifyDataArrived(String username, String device,
			String sensor, String channel, long timestamp, String value) {

		DeviceId deviceId = new DeviceId(username, device, sensor, channel);		
		DataUpload.LOG.info("Data received | " + deviceId.toString() + " " + timestamp + " " + value);

		ArrayList<DeviceEventListener> listListener = mapListeners.get(deviceId
				.toString());
		if (null == listListener)
			return;

		//DataUpload.LOG.info("notifyWaveSegmentArrived.. Listeners "
			//	+ listListener.size() + "\n");

		for (DeviceEventListener listener : listListener) {
			DataUpload.LOG.info("Notifying | " + deviceId + "'s listener "  + listener.getJobDetail().getKey().toString());			
			listener.deviceDataReceived(deviceId, timestamp, value);
		}
	}

	/**
	 * 
	 * @param deviceId
	 * @param newListener
	 */
	public void addDeviceEventListener(DeviceId deviceId,
			DeviceEventListener newListener) {

		//System.out.println("addDeviceEventListener.. DeviceId "
			//	+ deviceId.toString());
		
		ArrayList<DeviceEventListener> listListener = mapListeners.get(deviceId
				.toString());

		if (null == listListener) {
			listListener = new ArrayList<DeviceEventListener>();
		}
		listListener.add(newListener);
		mapListeners.put(deviceId.toString(), listListener);

		ArrayList<DeviceEventListener> listListener1 = mapListeners
				.get(deviceId.toString());
		
		TaskletScheduler.LOG.info("Total #registered listeners "+ listListener1.size());
	}

	/**
	 * 
	 * @param deviceId
	 * @param jobdetail
	 */
	public boolean removeDeviceEventListener(DeviceId deviceId,
			JobDetail jobdetail) {

		ArrayList<DeviceEventListener> listListener = mapListeners.get(deviceId
				.toString());

		boolean result = false;

		if (null != listListener) {
			
			for (DeviceEventListener listener : listListener) {				
						
				if (listener.getJobDetail().equals(jobdetail)) {					
					result = listListener.remove(listener);					
					break;
				}
			}
		}		
		return result;
	}

}
