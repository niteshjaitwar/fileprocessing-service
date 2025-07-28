package com.adp.esi.digitech.file.processing.repo.specification;

import org.springframework.data.jpa.domain.Specification;

import com.adp.esi.digitech.file.processing.entity.FPSRequestEntity;
import com.adp.esi.digitech.file.processing.entity.FPSRequestQueueEntity;



public class FPSRequestSpecification {
	
	public static Specification<FPSRequestEntity> joinQueue() {
		return (root, query, criteriaBuilder) -> {
			var queueRoot = query.from(FPSRequestQueueEntity.class);
			
			var uniqueIdPredicate = criteriaBuilder.equal(root.get("uniqueId"), queueRoot.get("uniqueId"));
			var uuidPredicate = criteriaBuilder.equal(root.get("uuid"), queueRoot.get("uuid"));
			var joinCondition =  criteriaBuilder.and(uniqueIdPredicate, uuidPredicate);
			 return criteriaBuilder.and(joinCondition);
		};
	}
	
	public static Specification<FPSRequestEntity> hasBu(String bu) {
		return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("bu"), bu);
	}

	public static Specification<FPSRequestEntity> hasPlatform(String platform) {
		return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("platform"), platform);
	}

	public static Specification<FPSRequestEntity> hasDataCategory(String dataCategory) {
		return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("dataCategory"), dataCategory);
	}
	
	public static Specification<FPSRequestEntity> hasCreatedBy(String createdBy) {
		return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("createdBy"), createdBy);
	}

	public static Specification<FPSRequestEntity> hasSourceType(String sourceType) {
		return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("sourceType"), sourceType);
	}
}
