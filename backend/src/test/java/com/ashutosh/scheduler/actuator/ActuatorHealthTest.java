package com.ashutosh.scheduler.actuator;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ActuatorHealthTest {

    @LocalManagementPort
    private int managementPort;

    @Test
    @DisplayName("Actuator health endpoint should return UP and expose database status")
    void testActuatorHealthEndpoint() {
        RestClient restClient = RestClient.create();
        String response = restClient.get()
            .uri("http://localhost:" + managementPort + "/actuator/health")
            .retrieve()
            .body(String.class);

        assertThat(response).isNotNull();
        assertThat(response).contains("\"status\":\"UP\"");
        assertThat(response).contains("\"db\"");
    }
}
