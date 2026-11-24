package loadtest.kafka.spring;

import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka-Lasttest mit Spring Boot. Die Optionen sind dieselben wie bei der Python-Variante
 * und werden nicht als Spring-Properties interpretiert.
 */
@SpringBootApplication
public class SpringLoadTestApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringLoadTestApplication.class);
        app.setAddCommandLineProperties(false);
        // Bei SIGTERM beendet der Lasttest sich selbst und schreibt noch seinen Abschlussbericht.
        app.setRegisterShutdownHook(false);
        System.exit(SpringApplication.exit(app.run(args), new ExitCodeGenerator[0]));
    }
}
