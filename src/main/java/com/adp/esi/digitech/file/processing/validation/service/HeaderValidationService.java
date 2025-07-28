package com.adp.esi.digitech.file.processing.validation.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.adp.esi.digitech.file.processing.exception.MetadataValidationException;
import com.adp.esi.digitech.file.processing.model.Metadata;

import lombok.extern.slf4j.Slf4j;

@Service("headerValidatorService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class HeaderValidationService extends AbstractValidationService<Metadata> {
	
	@Override
	public void validate(Metadata data) throws MetadataValidationException {		
		log.info("HeaderValidatorService -> validate() Started validating Headers, uniqueId = {}, difference = {}",requestContext.getUniqueId());
		
		Assert.notEmpty(data.getDbHeaders(),"DB Headers collection must not be empty: it must contain at least 1 element");
		Assert.notEmpty(data.getReqheaders(),"Req Headers collection must not be empty: it must contain at least 1 element");
		
		List<String> difference = data.getDbHeaders().parallelStream().filter(header -> !data.getReqheaders().contains(header)).collect(Collectors.toList());
		if(!difference.isEmpty()) {
			var metadataValidationException = new MetadataValidationException("Missing or Invalid Field headers found for the request = " + String.join(",", difference));
			metadataValidationException.setRequestContext(requestContext);
			throw metadataValidationException;
		}
		
		log.info("HeaderValidatorService -> validate() Completed validating Headers, uniqueId = {}, difference = {}",requestContext.getUniqueId(), difference);
	}
	

}
