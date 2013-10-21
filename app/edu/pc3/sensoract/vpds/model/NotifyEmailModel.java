/**
 * 
 */
package edu.pc3.sensoract.vpds.model;

import com.google.code.morphia.annotations.Converters;

@Converters(NotifyEmailModelConverter.class)
public class NotifyEmailModel {

	public String toemail = null;
	public String subject = null;
	public String message = null;

	public NotifyEmailModel(String toId, String subject, String message) {
		this.toemail = toId;
		this.subject = subject;
		this.message = message;
	}

}
