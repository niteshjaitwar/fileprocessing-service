package com.adp.esi.digitech.file.processing.model;

import com.adp.esi.digitech.file.processing.enums.Status;

import lombok.Getter;

@Getter
public final class ApiResponse<T> {
	
	private ErrorResponse error;
	private T data;
	private Status status;
	private String message;
	
	private ApiResponse(Status status, T data) {
        this.data = data;
        this.status = status;
    } 
	
	private ApiResponse(Status status, String message, T data) {
        this.data = data;
        this.status = status;
        this.message = message;
    } 
	
	private ApiResponse(Status status, ErrorResponse error) {
        this.error = error;
        this.status = status;
    }
	
    public static <T> ApiResponse<T> success(
            Status status,
            T data) {
        return new ApiResponse<>(status, data);
    }
	
    public static <T> ApiResponse<T> success(
            Status status,
            String message,
            T data) {
        return new ApiResponse<>(status,message, data);
    }
 
    
    public static <T> ApiResponse<T> error(
            Status status,
            ErrorResponse error) {
        return new ApiResponse<>(status, error);
    }

}
