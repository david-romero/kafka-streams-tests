package com.davromalc.kafka.streams.core.mail;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.stereotype.Component;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import com.davromalc.kafka.streams.model.menu.Menu;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MailBuilder {
	
	private final TemplateEngine templateEngine;
	
	@Value(value = "${spring.mail.subject}")
	private String mailSubject;
	
	@Value(value = "${spring.mail.from}")
	private String mailFrom;
	
	public MailBuilder(TemplateEngine templateEngine) {
		this.templateEngine = templateEngine;
	}

	public MimeMessagePreparator build(Menu menu) {
		return mimeMessage -> {
	        MimeMessageHelper messageHelper = new MimeMessageHelper(mimeMessage);
	        messageHelper.setFrom(mailFrom);
	        messageHelper.setTo(menu.getSubscriptor().getEmail());
	        messageHelper.setSubject(mailSubject);
	        messageHelper.setText(buildBody(menu), true);
	    };
	}
	
	private String buildBody(Menu menu) {
		log.info("Sending email to [{}]", menu.getSubscriptor().getEmail());
        Context context = new Context();
        context.setVariable("menu", menu);
        return templateEngine.process("index", context);
    }
	
}
