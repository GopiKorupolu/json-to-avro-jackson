package com.example.jsontoavro;

import com.example.jsontoavro.controller.ConvertToAvroUisngJackson;
import com.example.jsontoavro.model.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.InputStream;

@SpringBootApplication
public class JsontoavroApplication {

	public static void main(String[] args) throws Exception {

		SpringApplication.run(JsontoavroApplication.class, args);

		ConvertToAvroUisngJackson c = new ConvertToAvroUisngJackson();

		//Generate AvroSchema object using jackson.dataformat.avro.AvroSchema
		Schema.Parser parser = new Schema.Parser();
		InputStream stream = new FileInputStream("xml.avsc");
		Schema raw = parser.parse(stream);
		AvroSchema avroSchema = new AvroSchema(raw);

		//Convert json String to JsonObject using ObjectMapper
		String jsonString = "{\"name\":\"Gopi\",\"id\":\"12345\"}";
		ObjectMapper mapper = new ObjectMapper();
		Employee empJsonObj = mapper.readValue(jsonString, Employee.class);


		c.convertToavro(empJsonObj, avroSchema);
	}

}
