package com.adp.esi.digitech.file.processing.notification.service;

import jakarta.mail.BodyPart;
import jakarta.mail.Message;
import jakarta.mail.Multipart;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.notification.model.Email;

@Service
@Slf4j
public class EmailService {

	@Autowired
	JavaMailSender javaMailSender;

	public void send(Email email) {
		if (email != null) {
			try {

				MimeMessage msg = javaMailSender.createMimeMessage();
				msg.setFrom(new InternetAddress(email.getFrom()));
				msg.setReplyTo(InternetAddress.parse(email.getFrom()));
				msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(email.getTo()));

				if ((email.getCc() != null) && (email.getCc().length() > 0)) {
					msg.setRecipients(Message.RecipientType.CC, InternetAddress.parse(email.getCc()));
				}

				if ((email.getBcc() != null) && (email.getBcc().length() > 0)) {
					msg.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(email.getBcc()));
				}

				msg.setSubject(email.getSubject());

				Multipart mail = new MimeMultipart();

				BodyPart bodyPart = new MimeBodyPart();
				bodyPart.setContent(email.getBody(), "text/html;charset=windows-1252");
				mail.addBodyPart(bodyPart);

				msg.setContent(mail);
				javaMailSender.send(msg);

			} catch (Exception ex) {
				log.error("EmailService - send(), error = {}", ex.getMessage());
			}
		}
	}
}
