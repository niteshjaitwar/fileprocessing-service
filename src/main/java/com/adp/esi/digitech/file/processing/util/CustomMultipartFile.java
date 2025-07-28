package com.adp.esi.digitech.file.processing.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.web.multipart.MultipartFile;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class CustomMultipartFile implements MultipartFile {
	
	private byte[] input;
	private String fileName;
	private String contentType;

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return fileName;
	}

	@Override
	public String getOriginalFilename() {
		// TODO Auto-generated method stub
		return fileName;
	}

	@Override
	public String getContentType() {
		// TODO Auto-generated method stub
		return contentType;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return input == null || input.length == 0;
	}

	@Override
	public long getSize() {
		// TODO Auto-generated method stub
		return input.length;
	}

	@Override
	public byte[] getBytes() throws IOException {
		// TODO Auto-generated method stub
		return input;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		return new ByteArrayInputStream(input);
	}

	@Override
	public void transferTo(File dest) throws IOException, IllegalStateException {
		 try(FileOutputStream fos = new FileOutputStream(dest)) {
	            fos.write(input);
	      }	
	}

}
