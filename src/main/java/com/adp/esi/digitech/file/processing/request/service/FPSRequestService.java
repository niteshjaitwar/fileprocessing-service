package com.adp.esi.digitech.file.processing.request.service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.entity.FPSRequestEntity;
import com.adp.esi.digitech.file.processing.enums.RequestStatus;
import com.adp.esi.digitech.file.processing.enums.Status;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.model.FPSRequest;
import com.adp.esi.digitech.file.processing.repo.FPSRequestRepository;
import com.adp.esi.digitech.file.processing.repo.specification.FPSRequestSpecification;

@Service
public class FPSRequestService {
	
	@Autowired
	FPSRequestRepository fpsRequestRepository;
	
	@Autowired
	FPSRequestQueueService fpsRequestQueueService;
	
	@Autowired
	ModelMapper modelMapper;
	
	public List<FPSRequest> findBy(FPSRequest fpsRequest, boolean isAdmin) {
		
		Specification<FPSRequestEntity> spec = Specification.where(null);
		
		spec = spec.and(FPSRequestSpecification.joinQueue())
				.and(FPSRequestSpecification.hasBu(fpsRequest.getBu()))
					.and(FPSRequestSpecification.hasPlatform(fpsRequest.getPlatform()))
					.and(FPSRequestSpecification.hasDataCategory(fpsRequest.getDataCategory()))					
					.and(FPSRequestSpecification.hasSourceType(fpsRequest.getSourceType()));
		if(!isAdmin) {
			spec = spec.and(FPSRequestSpecification.hasCreatedBy(fpsRequest.getCreatedBy()));
		}
		
		var rows = fpsRequestRepository.findAll(spec);
				
		//fpsRequestRepository.findByBuAndPlatformAndDataCategory(fpsRequest.getBu(),	fpsRequest.getPlatform(), fpsRequest.getDataCategory());			
		return rows.stream().map(item -> modelMapper.map(item, FPSRequest.class)).collect(Collectors.toList());
		
	}
	
	public FPSRequest findBy(Long id) {
		if(id == null || id.longValue() <= 0)
			throw new ConfigurationException("id should be greater than 0");
		
		var item = fpsRequestRepository.findById(id).orElseThrow(() -> new ConfigurationException("No data found for given id = "+ id.longValue()));		
		return modelMapper.map(item, FPSRequest.class);
	}

	public FPSRequest add(FPSRequest fpsRequest) {
		var temp = modelMapper.map(fpsRequest, FPSRequestEntity.class);		
		return modelMapper.map(fpsRequestRepository.save(temp), FPSRequest.class);
	}

	public FPSRequest update(FPSRequest b) {
		var optinalExisting = fpsRequestRepository.findByBuAndDataCategoryAndPlatformAndUniqueIdAndUuid(b.getBu(),
				b.getDataCategory(), b.getPlatform(), b.getUniqueId(), b.getUuid());
		if(optinalExisting.isPresent()) {
			var existing = optinalExisting.get();
			existing.setStatus(b.getStatus());
			existing.setErrorType(b.getErrorType());
			existing.setErrorDetails(b.getErrorDetails());
			existing.setModifiedBy("DVTS-Admin");
			existing.setModifiedDate(new Date());	
			return modelMapper.map(fpsRequestRepository.save(existing), FPSRequest.class);
		}
		
		return null;
	}
	
	public void reprocessRequest(String uniqueId, String uuid, Long requestID) {		
		var obj = fpsRequestQueueService.findBy(uniqueId, uuid);
		if (Objects.nonNull(obj)) {
			fpsRequestQueueService.updateMesasgeStatus(obj.getId(), Status.PENDING.getStatus());
			fpsRequestRepository.findById(requestID).ifPresent(request -> {
				request.setStatus(RequestStatus.ReSubmitted.getRequestStatus());
				fpsRequestRepository.save(request);
			});
		}
	}

}
