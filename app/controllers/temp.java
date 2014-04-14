package controllers;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
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

import play.Play;
import play.libs.WS;
import play.libs.WS.HttpResponse;
import play.libs.WS.WSRequest;
import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.api.request.TaskletAddFormat;
import edu.pc3.sensoract.vpds.api.response.QueryDataOutputFormat;
import edu.pc3.sensoract.vpds.data.DataArchiever;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import edu.pc3.sensoract.vpds.model.TaskletModel;
import edu.pc3.sensoract.vpds.tasklet.Email;
import edu.pc3.sensoract.vpds.tasklet.SMSGateway;
import edu.pc3.sensoract.vpds.tasklet.TaskletScheduler;
import edu.pc3.sensoract.vpds.util.JsonUtil;
import edu.ucla.nesl.sensorsafe.db.informix.InformixDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;

public class temp  extends SensorActAPI {
	
	public static String COMPUTED_PATH = "./conf/computed/";
	
	public String getFileData(File file) {
		
		try {
			//File file = Play.getFile(COMPUTED_PATH+filename);
			//FileReader fr = new FileReader(file);
			
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
	
	static {
		
		try {
			
			//System.out.println("connecting to Ifx.....");
			//InformixDatabaseDriver.initializeConnectionPool();
			////InformixUserDatabaseDriver.initializeDatabase();
			//InformixStreamDatabaseDriver.initializeDatabase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// download one channel from ifx and upload into mongo
	public void ifxDownload() {

		//nesl_owner__NESL_Veris__Current__Current0Test
		// first 4 2013-03-07 17:08:52.00000 2.108602523804
		// count 8409837
		
		String username = "nesl_owner";
		String device = "NESL_Veris";
		String sensor = "Current";
		String channel = "Current0Test";
		
		DateTime dtStart = new DateTime(2013, 10, 7, 0, 0);		
		DateTime dtEnd =   new DateTime(2013, 11, 7, 0, 0);
		
		QueryDataOutputFormat data;
		long start, end, time , total = 0;
		
		while(dtStart.getMillis() < dtEnd.getMillis()) {
			
			System.out.print("Fetching for " + dtStart.toString());
			
			start = dtStart.getMillis();
			end = dtStart.plusDays(1).getMillis();
			
			data = DataArchiever.getDatapoints(username, device, sensor, channel, start, end, "");
			//System.out.println(JsonUtil.json1.toJson(data));
			System.out.println("  " + data.datapoints.size() + " datapoitns ");
			System.out.print("Storing " + data.datapoints.size() + " datapoitns..");
			
			for (QueryDataOutputFormat.Datapoint dp : data.datapoints) {				
				time = Long.parseLong(dp.time);
				DBDatapoint.save(username, device, sensor, channel, time, dp.value);
			}			
			total = total + data.datapoints.size();
			System.out.println(" done!       total " + total);
			
			dtStart = dtStart.plusDays(1);
		}
		
		//System.out.println(JsonUtil.json1.toJson(data));
		
	}
	
	public void ifxTest() {
		
		String username = "samy";
		String device = "device";
		String sensor = "sensor";
		String channel = "channel";
		
		DateTime start = DateTime.now();				
		
		for(int i=0; i<10; i++) {			
			DateTime dt = start.plusMillis(i);
			//Timestamp timeStamp = new Timestamp(dt.getMillis());			
			DataArchiever.storeDatapoint(username, device, sensor, channel, dt.getMillis(), ""+(i*1.0));
		}
		
		DateTime dtStart = start.plusMillis(0); 
		DateTime dtEnd = start.plusMillis(10000000);
		
		System.out.println( DateTime.now().toString()+ " querying..");
		QueryDataOutputFormat out = DataArchiever.getDatapoints(username, device, sensor, channel, 
				dtStart.getMillis(), dtEnd.getMillis(), "" );
		
		System.out.println( DateTime.now().toString()+ " done..");
		
		String s1 = JsonUtil.json1.toJson(out);	
		System.out.println(s1);
	}
	
	
	public String uname = "samy";
	public String dsname = "test_channel";
	
	public void informix() {
		List<Channel> chList  = new ArrayList<Channel>();		
		chList.add(new Channel("ch1", "float"));

		Stream st = new Stream(1, dsname, "tags", chList);
		
		try {
			InformixStreamDatabaseDriver ifx = new InformixStreamDatabaseDriver();
			ifx.createStream(uname, st);
			ifx.close();
		} catch(Exception e) {
			System.out.println("Error create : " + e.getMessage());
		}
		
		
		try {
			//Log.info(DateTime.now().toString() + " Inserting...");
			DateTime start = DateTime.now();
			
			long t1 = DateTime.now().getMillis();
			long t2 = DateTime.now().getMillis();
			long t3 = DateTime.now().getMillis();
			
			DateTime iStart = start;
			
			InformixStreamDatabaseDriver ifx = new InformixStreamDatabaseDriver();
			
			int i=0;
			while(true) {				
				DateTime now = start.plusMillis(i++);				
				Timestamp timeStamp = new Timestamp(now.getMillis());
								
				String d = "{\"timestamp\" : \"" + timeStamp.toString() + "\", \"tuple\" : [" + i + "]}"; 
				//String d = "[\"" + timeStamp.toString() + "\", " + i + "]";
				System.out.println(d);
				ifx.addTuple(uname, dsname, d);
				ifx.close();
				
				if(i%1000 == 0 ) {
					t1 = DateTime.now().getMillis();					
					iStart = now;
					//Log.info(DateTime.now().toString() + " Querying...");					
					String stt = new Timestamp(start.getMillis()).toString();
					String ent = new Timestamp(t1).toString();					
					t2 = DateTime.now().getMillis();
					ifx.prepareQuery(uname, uname, dsname,  stt, ent, "", 0,0);
					t3 = DateTime.now().getMillis();
				}
				break;
			}
			//renderJSON(obj);
			
		} catch(Exception e) {
			System.out.println(e.getMessage());
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
