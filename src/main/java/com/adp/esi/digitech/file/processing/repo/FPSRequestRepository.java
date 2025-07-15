package com.adp.esi.digitech.file.processing.repo;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import com.adp.esi.digitech.file.processing.entity.FPSRequestEntity;

@Repository
public interface FPSRequestRepository extends JpaRepository<FPSRequestEntity, Long>, JpaSpecificationExecutor<FPSRequestEntity> {

	public Optional<FPSRequestEntity> findByBuAndDataCategoryAndPlatformAndUniqueIdAndUuid(String bu, String dataCategory,
			String platform, String uniqueId, String uuid);
	
	public Optional<FPSRequestEntity> findByUniqueIdAndUuid(String uniqueId, String uuid);
	
	public List<FPSRequestEntity> findByBuAndDataCategoryAndPlatformAndUniqueId(String bu, String dataCategory,
			String platform, String uniqueId);
	
	public List<FPSRequestEntity> findByBuAndPlatformAndDataCategory(String bu, String platform, String dataCategory);

}
