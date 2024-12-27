
package solutions.Infy.kafka.connect.sendgrid.sink;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solutions.Infy.kafka.connect.sendgrid.Version;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 *
 */

public class SendGridSinkTask extends SinkTask {
    private SendGridWriter writer;
    private static final Logger log = LoggerFactory.getLogger(SendGridSinkTask.class);

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info(asciiArt());
        log.info("starting the task");
        log.trace("Starting the Task with properties -{}", properties);
        initWriter(properties);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Info Putting {} to Sendgrid.", records);
        log.trace("Trace : Putting {} to SendGrid.", records);
        writer.write(records);

        log.info("Info log after writing the record- {}",records);
    }

    @Override
    public void stop() {
        log.info("Stopping SendGridSinkTask.");
    }

    private void initWriter(final Map<String, String> config) {
        log.info("Initializing properties in Task ");
        log.info("Initializing properties in Task with config details -{}", config);
        this.writer = new SendGridWriter(config);
    }


    @SuppressWarnings("resource")
    private String asciiArt() {
        return new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/sendgrid-sink-ascii.txt")))
                .lines()
                .collect(Collectors.joining("\n"));
    }
}