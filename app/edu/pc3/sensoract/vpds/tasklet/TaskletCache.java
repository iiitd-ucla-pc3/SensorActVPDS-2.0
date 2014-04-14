package edu.pc3.sensoract.vpds.tasklet;

import java.util.HashMap;
import java.util.Map;

public class TaskletCache {
	
	private static Map<String,String> map = new HashMap<String,String>();
	
	public static String put(String taskletId, String key, String value) {
		key = taskletId + ":" + key;
		return map.put(key, value);
	}

	public static String get(String taskletId, String key) {
		key = taskletId + ":" + key;
		return map.get(key);
	}

	public static String remove(String taskletId, String key) {
		key = taskletId + ":" + key;
		return map.remove(key);
	}

	// remove all keys that starts with jobKey
	public static void removePrefixAll(String taskletId) {		
		taskletId = taskletId + ":";		
		for(String key: map.keySet()) {			
			if(key.startsWith(taskletId)) {
				map.remove(key);
			}
		}		
	}
}
