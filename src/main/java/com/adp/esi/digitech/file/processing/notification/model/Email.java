package com.adp.esi.digitech.file.processing.notification.model;

import java.util.List;


public class Email {
	private String from;
	private String to;
	private String cc;
	private String bcc;
	private String replyTo;
	private String subject;
	private String body;
	private List attachments;
	
	public Email(){
		
	}	
	public Email(String from, String to, String cc, String bcc, String replyTo,
			String subject, String body, List attachments) {
		super();
		this.from = from;
		this.to = to;
		this.cc = cc;
		this.bcc = bcc;
		this.replyTo = replyTo;
		this.subject = subject;
		this.body = body;
		this.attachments = attachments;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
	public String getCc() {
		return cc;
	}
	public void setCc(String cc) {
		this.cc = cc;
	}
	public String getBcc() {
		return bcc;
	}
	public void setBcc(String bcc) {
		this.bcc = bcc;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}		
	public String getReplyTo() {
		return replyTo;
	}
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public List getAttachments() {
		return attachments;
	}
	public void setAttachments(List attachments) {
		this.attachments = attachments;
	}
}