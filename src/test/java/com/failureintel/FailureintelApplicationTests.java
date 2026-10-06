package com.failureintel;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import com.failureintel.test.support.PostgresTestContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
class FailureintelApplicationTests {

	@DynamicPropertySource
	static void postgresProperties(DynamicPropertyRegistry registry) {
		PostgresTestContainer.registerDatabase(registry, "failureintel_context_test");
	}

	@Test
	void contextLoads() {
	}

}
