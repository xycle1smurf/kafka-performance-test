package loadtest.kafka.spring;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import loadtest.kafka.core.LoadTest;

/** Führt den Lasttest nach dem Start des Kontexts aus und liefert dessen Exit-Code. */
@Component
public class LoadTestRunner implements CommandLineRunner, ExitCodeGenerator {

    private final SpringKafkaEngine engine;
    private int exitCode;

    public LoadTestRunner(SpringKafkaEngine engine) {
        this.engine = engine;
    }

    @Override
    public void run(String... args) {
        exitCode = LoadTest.main(args, engine);
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }
}
