package edu.ucla.nesl.sensorsafe.informix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import edu.ucla.nesl.sensorsafe.db.DatabaseDriver;

abstract public class InformixDatabaseDriver implements DatabaseDriver {

	/*
	dbc:informix-sqli://[{ip-address|host-name}:{port-number|service-name}][/dbname]:
		   INFORMIXSERVER=servername[{;user=user;password=password]
		|CSM=(SSO=database_server@realm,ENC=true)}
		   [;name=value[;name=value]...]
		*/		   
	// TODO Read this from properties file.
	private static final String HOST_SERVER_DNS_NAME = "128.97.93.31"; //"127.0.0.1";
	private static final String HOST_SERVER_PORT = "9088";
	private static final String DB_SERVER_NAME = "ol_informix1170";
	private static final String DB_NAME = "sensoract";
	private static final String DB_USERNAME = "informix";
	private static final String DB_PASSWORD = "password";

	protected static final String DB_CONNECT_URL = "jdbc:informix-sqli://" 
			+ HOST_SERVER_DNS_NAME + ":" + HOST_SERVER_PORT + "/" + DB_NAME +
			":INFORMIXSERVER=" + DB_SERVER_NAME + 
			";user=" + DB_USERNAME + 
			";password=" + DB_PASSWORD;
	
	 //jdbc:informix-sqli://myhost:1533:informixserver=myserver;user=<username>;password=<password>'
	 //jdbc:informix-sqli://localhost:1533/sensoract:INFORMIXSERVER=ol_informix1170;user=;password=

	protected Connection conn;

	@Override
	public void connect() throws SQLException, ClassNotFoundException {
		if (conn == null) {
			Class.forName("com.informix.jdbc.IfxDriver");
			String url = "jdbc:informix-sqli://128.97.93.31:9088/sensoract:informixserver=ol_informix1170;user=informix;password=password";
			System.out.println(DB_CONNECT_URL);
			System.out.println(url);
			conn = DriverManager.getConnection(url);
			initializeDatabase();
		}
	}

	@Override
	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}
	
	abstract protected void initializeDatabase() throws SQLException, ClassNotFoundException;
}