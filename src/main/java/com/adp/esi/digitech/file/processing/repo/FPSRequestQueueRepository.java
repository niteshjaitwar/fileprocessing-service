package com.adp.esi.digitech.file.processing.repo;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.adp.esi.digitech.file.processing.entity.FPSRequestQueueEntity;

public interface FPSRequestQueueRepository extends JpaRepository<FPSRequestQueueEntity, Long> {
	
	@Transactional	
	@Query(value = """
			SELECT * FROM MS_FPS_REQUEST_QUEUE
			WHERE STATUS = :status AND ROWNUM = 1
			ORDER BY CREATED_DATE_TIME ASC 
			 FOR UPDATE SKIP LOCKED
			""", nativeQuery = true)
	public Optional<FPSRequestQueueEntity> fetchNextPendingMessage(@Param("status") String status);
	
	
	@Transactional
	@Modifying
	@Query(value = """			
			UPDATE MS_FPS_REQUEST_QUEUE 
			SET STATUS = :status, 
			MODIFIED_DATE_TIME = CURRENT_TIMESTAMP 
			WHERE ID = :id
			""", nativeQuery = true)
	public void updateMessageStatus(@Param("id") Long id, @Param("status") String status);
	
	
	public Optional<FPSRequestQueueEntity> findByUniqueIdAndUuid(String uniqueId, String uuid);

	/*
	 * @Query(value = """
			SELECT * FROM MS_FPS_REQUEST_QUEUE
				WHERE STATUS = :status
				ORDER BY CREATED_DATE_TIME
				FETCH FIRST 1 ROW ONLY
				FOR UPDATE SKIP LOCKED
			""", nativeQuery = true)
	@Query("UPDATE FPSRequestQueueEntity q SET q.status = 'PROCESSING', q.modifiedDate = CURRENT_TIMESTAMP " +
	"WHERE q.id = (SELECT q2.id FROM FPSRequestQueueEntity q2 WHERE q2.status = 'PENDING' ORDER BY q2.createdDate ASC LIMIT 1) " +
			"RETURNING q")
			
			@Query(value = """
			UPDATE MS_FPS_REQUEST_QUEUE
			SET STATUS = 'PROCESSING', MODIFIED_DATE_TIME = CURRENT_TIMESTAMP
			WHERE ID = (
				SELECT ID FROM MS_FPS_REQUEST_QUEUE
				WHERE STATUS = :status
				ORDER BY CREATED_DATE_TIME
				FETCH FIRST 1 ROW ONLY
				FOR UPDATE SKIP LOCKED
			)
			RETURNING ID, UNIQUE_ID, UUID, STATUS, CREATED_DATE_TIME, MODIFIED_DATE_TIME, REQUEST_PAYLOAD
			""", nativeQuery = true)
	*/
}
