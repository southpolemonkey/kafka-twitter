package com.github.southpolemonkey.kafka.stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/*
  Test jackson-databind package for parsing json
  https://github.com/FasterXML/jackson-databind
 */

public class MyValue {
    public String name;
    public int age;

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory f = mapper.getFactory();

        try {
            File jsonFile = new File("json_parse.json");

            JsonParser p = f.createParser(jsonFile);

            JsonToken t = p.nextToken();

            while (t != null) {
                t = p.nextToken(); // JsonToken.FIELD_NAME
                if ((t != JsonToken.FIELD_NAME) || !"message".equals(p.getCurrentName())) {
                    // handle error
                }

                String msg = p.getText();
                System.out.printf("My message to you is: %s!\n", msg);
            }

            p.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
