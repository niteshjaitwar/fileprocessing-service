package com.adp.esi.digitech.file.processing.json.deserializer;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import lombok.extern.slf4j.Slf4j;

@JsonComponent
@Slf4j
public class FormTextDeserializer extends JsonDeserializer<JSONObject> {

	@Override
	public JSONObject deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JacksonException {		
		
		JSONArray formText = new JSONArray(p.getText());
		log.info("FormTextDeserializer -> Completed deserialize");		
		/*
		JSONObject jsonObject = new JSONObject();
		if(formText !=null && !formText.isEmpty())
			formText.forEach(item -> {
				JSONObject obj = (JSONObject) item;
				if(obj.has("text"))
					jsonObject.put("text", obj.get("text"));
				else if(obj.has("data_grid"))
					jsonObject.put("data_grid", obj.get("data_grid"));
				else if(obj.has("tabs"))
					jsonObject.put("tabs", obj.get("tabs"));
			});
		
		return jsonObject;
		*/
		return formText.getJSONObject(0);
	}
	
	
}
