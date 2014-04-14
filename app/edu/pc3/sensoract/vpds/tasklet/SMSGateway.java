package edu.pc3.sensoract.vpds.tasklet;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import play.Play;

import com.twilio.sdk.TwilioRestClient;
import com.twilio.sdk.resource.factory.SmsFactory;
import com.twilio.sdk.resource.instance.Sms;

public class SMSGateway {

	private static final Logger LOG = LuaToJavaFunctionMapper.LOG;
	private static final String NAME = SMSGateway.class.getSimpleName();

	private static TwilioRestClient twilioRestClient = null;
	private static SmsFactory messageFactory = null;
	private static String from = null;

	static {
		try {
			from = Play.configuration.getProperty("sms.from");
			String sid = Play.configuration.getProperty("sms.account.sid");
			String token = Play.configuration.getProperty("sms.auth.token");

			twilioRestClient = new TwilioRestClient(sid, token);
			messageFactory = twilioRestClient.getAccount().getSmsFactory();
		} catch (Exception e) {
			LOG.error(NAME + "Error while inititalizing SMS Gateway API "
					+ e.getMessage());
		}
	}

	public static void sendSMS(String context, String to, String msg) {

		context = (null == context ? "" : context);
		
		LOG.info(context + " " + NAME + " Sending SMS to " + to + " " + msg);

		if (null == twilioRestClient || null == messageFactory || null == from) {
			LOG.error(context + " " + NAME + " Uninitialized SMS Gateway API");
			return;
		}

		if (null == msg || msg.length() < 1) {
			LOG.error(context + " " + NAME + " Empty message!");
			return;
		}

		try {
			// validate to and msg
			//long no = Long.parseLong(to);
			
			// Build a filter for the SmsList
			Map<String, String> params = new HashMap<String, String>();
			params.put("Body", msg);
			params.put("From", from); // https://www.twilio.com/user/account/phone-numbers/incoming
			params.put("To", to);

			SmsFactory messageFactory = twilioRestClient.getAccount()
					.getSmsFactory();
			Sms message = messageFactory.create(params);

			LOG.info(context + " " + NAME + " " + message.getStatus());
		} catch (Exception e) {
			LOG.error(context + " " + NAME + " Error " + e.getMessage());
		}
	}
}