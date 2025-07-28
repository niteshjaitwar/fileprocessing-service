package com.adp.esi.digitech.file.processing.notification.service;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.XMLConstants;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.notification.model.EmailNotificationData;
import com.adp.esi.digitech.file.processing.notification.model.EmailNotificationHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class EmailNotificationService {

	@Autowired
	private ResourceLoader resourceLoader;
	
	@Autowired
	private JavaMailSender emailSender;
	
	@Autowired
	EmailNotificationHelper emailNotificationHelper;
    
	@Async
	public void sendExceptionEmail(String to,EmailNotificationData templateData) throws IOException {	 
		log.info("EmailNotificationService - sendExceptionEmail() Started Processing Email, uniqueId = {}", templateData.getRequestContext().getUniqueId());
		 XmlMapper mapper=new XmlMapper();
	    	try {
	    			
	    			String dataString = mapper.writeValueAsString(templateData);
	    			String transformedBody = transformMailBody(dataString,true);
	    			if(!transformedBody.isBlank()) {
	    				String subject=getMailSubject(Status.FAILED.getStatus(), templateData.getRequestContext().getUniqueId(),templateData.getRequestContext().getBu(),templateData.getRequestContext().getPlatform(),templateData.getRequestContext().getDataCategory());
	    				sendHTMLMail(emailNotificationHelper.getFrom(), emailNotificationHelper.getTo(), subject, transformedBody);
	    				
	    			}
	    			log.info("EmailNotificationService - sendExceptionEmail() Completed Processing Email, uniqueId = {}", templateData.getRequestContext().getUniqueId());
				} catch (JsonProcessingException e) {
					log.error("EmailNotificationService - sendExceptionEmail() Error in Processing Email, uniqueId = {} message = {}", templateData.getRequestContext().getUniqueId(),e.getMessage());
				}
	    
	    
	}
	
	@Async
	public void sendEmailWithTemplateBody(String to,EmailNotificationData templateData) throws IOException {
		 log.info("EmailNotificationService - sendEmailWithTemplateBody() Started Processing Email, uniqueId = {}", templateData.getRequestContext().getUniqueId());	
		
		 	XmlMapper mapper=new XmlMapper();
	    	try {
	    		String dataString=mapper.writeValueAsString(templateData);
	    		String transformedBody= transformMailBody(dataString,false);
	    		if(!transformedBody.isBlank()) {
	    			String subject=getMailSubject(Status.SUCCESS.getStatus(), templateData.getRequestContext().getUniqueId(),templateData.getRequestContext().getBu(),templateData.getRequestContext().getPlatform(),templateData.getRequestContext().getDataCategory());
	    			sendHTMLMail(emailNotificationHelper.getFrom(), emailNotificationHelper.getTo(), subject, transformedBody);
	    			
	    		}
	    		log.info("EmailNotificationService - sendEmailWithTemplateBody() Completed Processing Email, uniqueId = {}", templateData.getRequestContext().getUniqueId());
			} catch (JsonProcessingException e) {
				log.error("EmailNotificationService - sendEmailWithTemplateBody() Error in Processing Email, uniqueId = {}, message = {}", templateData.getRequestContext().getUniqueId(), e.getMessage());
			}
	    	
	    	
	}
	
    public String transformMailBody(String xmlString, boolean forExceptionEmail) throws IOException {
		TransformerFactory factory = TransformerFactory.newInstance();
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
		StreamSource xsltStream = new StreamSource();
		if(forExceptionEmail) {
			xsltStream = new StreamSource(resourceLoader.getResource("classpath:DVTSExceptionEmail.xslt").getInputStream());
		} else {
			xsltStream = new StreamSource(resourceLoader.getResource("classpath:DVTSSuccessEmail.xslt").getInputStream());
		}
		StreamSource xmlStream = new StreamSource(new StringReader(xmlString));
	    StringWriter writer = new StringWriter();
	    StreamResult output = new StreamResult(writer);

	    try {
	    	Transformer transformer = factory.newTransformer(xsltStream);
	        transformer.transform(xmlStream, output);
	    }
	    catch (Exception e) {
	            log.error("EmailNotificationService - transformMailBody() forExceptionEmail = {} in Transforming Email, message = {}",forExceptionEmail, e.getMessage());
	    }
	    return  writer.toString();
    }
    
    public void sendHTMLMail(String from,String to,String subject,String mailBody) {
	    	MimeMessage mimeMsg=emailSender.createMimeMessage();
	    	try {
				MimeMessageHelper mimeHelper=new MimeMessageHelper(mimeMsg, false);
				mimeHelper.setTo(InternetAddress.parse(to));
				mimeHelper.setFrom(from);
				mimeHelper.setSubject(subject);
				mimeHelper.setText(mailBody, true);
				emailSender.send(mimeMsg);
	    	} catch (MessagingException e) {
				log.error("EmailNotificationService - sendHTMLMail() Error in Sending Email, subject = {}, message = {}", subject, e.getMessage());
			} catch(Exception e) {
				log.error("EmailNotificationService - sendHTMLMail() Error in Sending Email, subject = {}, message = {}", subject, e.getMessage());
			}
    }
    
    private String getMailSubject(String status,String uniqueId,String bu,String platform,String dataCategory) {
			return emailNotificationHelper.getSubject().replace("$Status$", status).replace("$uniqueId$", uniqueId);
	}
    
}
