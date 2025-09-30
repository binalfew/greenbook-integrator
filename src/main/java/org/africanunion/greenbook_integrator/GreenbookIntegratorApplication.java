package org.africanunion.greenbook_integrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GreenbookIntegratorApplication {
	public static void main(String[] args) {
		SpringApplication.run(GreenbookIntegratorApplication.class, args);
	}
}
