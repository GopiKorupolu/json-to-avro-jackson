package com.example.jsontoavro.controller;

import com.example.jsontoavro.model.Employee;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class ConvertToAvroUisngJackson {

    public void convertToavro(Employee empJsonObj, AvroSchema avroSchema){
try {
    AvroMapper mapper = new AvroMapper();

    //Convert to Avro Object Array
    byte[] avroData = mapper.writer(avroSchema)
            .writeValueAsBytes(empJsonObj);

    System.out.println("Json to Avro data:: "+  avroData);


    //Convert Avro date to Json Object
    Employee empl = mapper.reader(Employee.class)
            .with(avroSchema)
            .readValue(avroData);

    System.out.println("Avro to Json data:: " + empl);


}catch(Exception e){
    e.printStackTrace();
}


    }




}
