package controllers;

import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Period;

import java.io.*;

import play.Play;
import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.libs.WS.WSRequest;
import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.api.request.TaskletAddFormat;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.pc3.sensoract.vpds.model.TaskletModel;
import edu.pc3.sensoract.vpds.tasklet.Email;
import edu.pc3.sensoract.vpds.tasklet.SMSGateway;
import edu.pc3.sensoract.vpds.tasklet.TaskletScheduler;
import edu.pc3.sensoract.vpds.util.JsonUtil;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class temp  extends SensorActAPI {
	
	public static String COMPUTED_PATH = "./conf/computed/";
	
	public String getFileData(File file) {
		
		try {
			//File file = Play.getFile(COMPUTED_PATH+filename);
			FileReader fr = new FileReader(file);
			
			FileInputStream fis = new FileInputStream(file);
		    byte[] data = new byte[(int)file.length()];
		    fis.read(data);		    
		    String s = new String(data, "UTF-8");
			return s;
		}catch(Exception e) {
			e.printStackTrace();
		}
		return "";
	}


	// load all the all computed sensor scripts in ./conf/computed
	// file extension : .computed
	// script entension: .lua
	// name is filename
	public void loadComputedSensors() {
		
		class ComputedFilter implements FileFilter {
		    @Override
		    public boolean accept(File file) {
		      return !file.isHidden() && file.getName().endsWith(".computed");
		    }
		  }
		
		try {
			File dir = Play.getFile(COMPUTED_PATH);			
			
			File cFiles[] = dir.listFiles( new ComputedFilter());			
			for(File c : cFiles ) {
				
				String csData = getFileData(c);			

				TaskletAddFormat tasklet =  convertToRequestFormat(csData, TaskletAddFormat.class);				
				String luaF = c.getName().replace(".computed", ".lua");
				
				String luaData = getFileData( Play.getFile(COMPUTED_PATH+luaF) );

				//System.out.println(luaData.length());
				//System.out.println(luaData);
				
				tasklet.execute = luaData;
				tasklet.secretkey = Play.configuration.getProperty("owner.ownerkey");
				
				String username = null;
				if (userProfile.isRegisteredSecretkey(tasklet.secretkey)) {
					username = userProfile.getUsername(tasklet.secretkey);					
				}				
				//taskletAdd.doProcess(actuateDeviceJson);
				
				taskletAdd.preProcessTasklet(tasklet);
				
				taskletManager.removeTasklet(tasklet.secretkey, tasklet.taskletname);
				taskletManager.addTasklet(tasklet);
				
				TaskletModel taskletModel = taskletManager.getTasklet(
						tasklet.secretkey, tasklet.taskletname);
				
				TaskletScheduler.cancelTasklet(username+"."+tasklet.taskletname);				
				String taskletId = TaskletScheduler.scheduleTasklet(username, taskletModel);
				
				System.out.println(JsonUtil.json1.toJson(taskletModel));
				System.out.println(taskletId + " Scheduled successfully");
				
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}
	
/*	
	private static StreamDatabaseDriver streamDb;
	
	static {
		streamDb = InformixStreamDatabaseDriver.getInstance();
		try {
			System.out.println("connecting to Ifx.....");
			streamDb.connect();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public void informix() {
		List<Channel> chList  = new ArrayList<Channel>();		
		chList.add(new Channel("ch1", "float"));

		Stream st = new Stream(1,"nesl_owner__Test_Device1__Temperature__channel1", "tags", chList);
		
		try {
			streamDb.createStream(st);
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
		
		
		try {
			Log.info(DateTime.now().toString() + " Inserting...");
			DateTime start = DateTime.now();
			
			long t1 = DateTime.now().getMillis();
			long t2 = DateTime.now().getMillis();
			long t3 = DateTime.now().getMillis();
			
			DateTime iStart = start; 
			
			int i=0;
			while(true) {				
				DateTime now = start.plusMillis(i++);				
				Timestamp timeStamp = new Timestamp(now.getMillis());				
				streamDb.addTuple("ds", timeStamp.toString(), i+"");				
				if(i%1000 == 0 ) {
					t1 = DateTime.now().getMillis();
					Log.info(DateTime.now().toString() + " Inserted " + i + " " + (t1-t3));
					iStart = now;
					//Log.info(DateTime.now().toString() + " Querying...");					
					String stt = new Timestamp(start.getMillis()).toString();
					String ent = new Timestamp(t1).toString();					
					t2 = DateTime.now().getMillis();
					int size = streamDb.queryStream("ds", stt, ent, null);
					t3 = DateTime.now().getMillis();
					Log.info(DateTime.now().toString() + " Queried  " + size + " " + (t3-t2)+"\n");
				}
				break;
			}
			//renderJSON(obj);
			
		} catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}
	
*/	
	
	
	
	public static void dbtest() {
		
		long t = new Date().getTime();

		DBDatapoint dbp = new DBDatapoint();

		//dbp.drop();
		
		for (int v = 0; v <00000000; ++v) {
			
			dbp.push(t+v, v + "");
			
			try {
				Thread.sleep(0);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(v%1000 == 0 ) {
				System.out.println(new Date()+ " pushed# " + v);
			}
		}
		
		//t = 1373511533116l;		
		System.out.println("..........counting..");
		dbp.count();
		
		System.out.println("..........fetching..");
		dbp.fetch(t, t+10000000);
		
		System.out.println("..........indexing.");
		dbp.index();

		System.out.println("..........fetching..");
		dbp.fetch(t, t+10000000);
		
	}
	
	public static void testSMS() {
		
		SMSGateway.sendSMS("Testing", "+13102168156", "test");		
		SMSGateway.sendSMS("Testing", "", "test1");
		SMSGateway.sendSMS("Testing", "+13102168156", "");
		SMSGateway.sendSMS("Testing", "+918800215144", "test2");
		
		String path = "/home/samy/git/SensorActVPDS-2.0/plots/Power_30min1382672695364.png";
				
		Email email = new Email("pandarasamya@iiitd.ac.in", "Tasklet notification", "HELLO", null);
		long t1 = new Date().getTime();
		System.out.println("before notifyEmail..." + new Date().getTime());
		email.sendNow(null);
		long t2 = new Date().getTime();
		System.out.print(" notifyEmail :" + (t2-t1));
		System.out.println("after notifyEmail..." + new Date().getTime());
	}
	
	
	public void c2() {		
		DateTime dt1 = DateTime.now();
		DateTime dt2 = dt1.minusHours(10);
		
		Period period = new Period(dt1, dt2);		
		String diff =  String.format("%02d Hours and %02d Minutes", period.getHours(), period.getMinutes());

		Period period1 = new Period(dt2, dt1);		
		String diff1 =  String.format("%02d Hours and %02d Minutes", period.getHours(), period.getMinutes());
		
		System.out.println(diff + "  " + diff1);
		
		 int hh = Hours.hoursBetween(dt1, dt2).getHours();
		 int mm = Minutes.minutesBetween(dt1, dt2).getMinutes() % 60;
		 
		 System.out.println(hh + "  " + mm);
	}
	
	// to store the temporary codes
	 
	public void c1() {
		try {
			String url="http://www.FreeSMSGateway.com/api_send";					
			WSRequest wsr = WS.url(url);
			
			String nos[] = {"3102168156"};
			String to = URLEncoder.encode(JsonUtil.json.toJson(nos));
			
			System.out.println(JsonUtil.json.toJson(nos));
			
			String msg = URLEncoder.encode("hello", "UTF-8");
			
			//access_token:c88f1e57bc979bed936023bd989a8e0d, message:"hello", send_to:"post_contacts" post_contacts:["3102168156"]
					
			Map<String,Object> params = new HashMap<String,Object>();
			params.put("access_token", "c88f1e57bc979bed936023bd989a8e0d");
			params.put("message", msg);
			params.put("send_to", URLEncoder.encode("3102168156", "UTF-8"));
			//params.put("post_contacts", "[3102168156]");			
			
			System.out.println(JsonUtil.json1.toJson(params));			
			HttpResponse trainRes = wsr.params(params).post();			
			System.out.println(trainRes.getString());
			
			
			// twilio
			url="https://api.twilio.com/2010-04-01/2010-04-01/Accounts/AC29476e1873c5df338b6305f81db74424/Messages.json";					
			wsr = WS.url(url);
			
			Map<String,Object> params1 = new HashMap<String,Object>();
			params1.put("From", "+13106934038");
			params1.put("To", "+13102168156");
			params1.put("Body", "SensorAct msg");
			
			System.out.println(JsonUtil.json1.toJson(params1));
			
			//trainRes = wsr.params(params).post();
			
			//SMSGateway.sendSMS();			
			//System.out.println(new java.io.File(".").getPath());
			System.out.println(new java.io.File(".").getCanonicalPath());
			System.out.println("paly " + Play.applicationPath.getPath());
			
			String sms = Play.configuration.getProperty("sms", "na");
			System.out.println("SMS : " + sms);
			
			

		} catch (Exception e) {
			e.printStackTrace();
			
		}

	}
}
