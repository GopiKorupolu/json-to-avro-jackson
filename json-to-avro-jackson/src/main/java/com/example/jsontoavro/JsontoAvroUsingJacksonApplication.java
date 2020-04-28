package com.example.jsontoavro;

import com.example.jsontoavro.model.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.InputStream;

@SpringBootApplication
public class JsontoAvroUsingJacksonApplication {

    public static void main(String[] args) throws Exception {

        SpringApplication.run(JsontoAvroUsingJacksonApplication.class, args);

		JsontoAvroUsingJacksonApplication c = new JsontoAvroUsingJacksonApplication();

        //Generate AvroSchema object using jackson.dataformat.avro.AvroSchema
        Schema.Parser parser = new Schema.Parser();
        InputStream stream = new FileInputStream("xml.avsc");
        Schema raw = parser.parse(stream);
        AvroSchema avroSchema = new AvroSchema(raw);

        //Convert json String to JsonObject using ObjectMapper
        String jsonString = "{\"name\":\"Gopi\",\"id\":\"12345\"}";
        ObjectMapper mapper = new ObjectMapper();
        Employee empJsonObj = mapper.readValue(jsonString, Employee.class);

        //Convert Json to Avro
        byte[] avroData = c.convertJsonToavro(empJsonObj, avroSchema);
        System.out.println("Json to Avro data:: " + avroData);

        //Convert Avro to Json
        System.out.println("Avro to Json data:: " +
                c.convertAvroToJson(avroData, avroSchema));
    }

    public byte[] convertJsonToavro(Employee empJsonObj, AvroSchema avroSchema) {

        byte[] avroData = new byte[0];
        try {
            AvroMapper mapper = new AvroMapper();

            //Convert Json to Avro byte Array
            avroData = mapper.writer(avroSchema)
                    .writeValueAsBytes(empJsonObj);


        } catch (Exception e) {
            e.printStackTrace();
        }

        return avroData;
    }

    public Employee convertAvroToJson(byte[] avroData, AvroSchema avroSchema) {

        Employee empl = null;
        try {
            AvroMapper mapper = new AvroMapper();

            //Convert Avro byte array to Json Object
            empl = mapper.reader(Employee.class)
                    .with(avroSchema)
                    .readValue(avroData);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return empl;
    }


}
