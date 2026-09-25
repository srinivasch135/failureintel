package com.failureintel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class FailureintelApplication {

	public static void main(String[] args) {
		SpringApplication.run(FailureintelApplication.class, args);
	}

}
