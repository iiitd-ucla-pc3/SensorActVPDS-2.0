/**
 * 
 */
package edu.pc3.sensoract.vpds.tasklet;

import java.util.Observable;
import java.util.Observer;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;

import edu.pc3.sensoract.vpds.api.DataUpload;
import edu.pc3.sensoract.vpds.api.request.WaveSegmentFormat;

/**
 * @author samy
 * 
 */
public class DeviceEventListener {

	private JobDetail jobDetail = null;

	public JobDetail getJobDetail() {
		return jobDetail;
	}

	public DeviceEventListener(JobDetail jobDetail) {
		this.jobDetail = jobDetail;
	}

	public void deviceDataReceived(DeviceId deviceId, long timestamp, String value) {
		
		LuaScriptTasklet.LOG.info("Trigger request | " + deviceId.toString() + " " + timestamp + " " + value);
		LuaScriptTasklet.LOG.info("Triggering tasklet | " + jobDetail.getKey().toString());
		
		// put tasklet params
		jobDetail.getJobDataMap().put(LuaScriptTasklet.PARAM_TIME, timestamp+"");
		jobDetail.getJobDataMap().put(LuaScriptTasklet.PARAM_VALUE, value);

		/*
		for(String k : jobDetail.getJobDataMap().getKeys()) {
			System.out.println(k + " " + jobDetail.getJobDataMap().get(k));
		}
		*/
		TaskletScheduler.triggerTasklet(jobDetail);
	}
}

