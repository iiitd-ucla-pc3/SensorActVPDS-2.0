package edu.pc3.sensoract.vpds.data;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.pc3.sensoract.vpds.api.SensorActAPI;
import edu.pc3.sensoract.vpds.model.DBDatapoint;
import play.jobs.*;

//@On("0 0/1 * * * ?")
public class IndexerJob extends Job {
	
	// private static final Logger LOG = LuaToJavaFunctionMapper.LOG;
	private static final String NAME = IndexerJob.class.getSimpleName();	
	private static int delay = 10 * 1000;
	
	public void doJob() {
	
		SensorActAPI.log.info(NAME + " Started..");
		
		try {
			Set<String> collections =  DBDatapoint.getCollectionNames();			
			SensorActAPI.log.info(NAME + " " + collections.size() + " Collections found");			
			for(String col : collections) {
				//SensorActAPI.log.info(NAME + " ReIndexing " +col);				
				//TODO: Indexing is not required as Mongo automatically creates an index on _id
				//DBDatapoint.ensureIndex(col);
			}
			
		} catch (Exception e){
			SensorActAPI.log.error(NAME + " Error " + e.getMessage());
		}		
		SensorActAPI.log.info(NAME + " Ended..");		
    }
}
