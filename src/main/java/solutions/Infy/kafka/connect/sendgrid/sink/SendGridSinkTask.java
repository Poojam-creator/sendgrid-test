
package solutions.Infy.kafka.connect.sendgrid.sink;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import solutions.Infy.kafka.connect.sendgrid.Version;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 *
 */
@Slf4j
public class SendGridSinkTask extends SinkTask {

    private SendGridWriter writer;

    private ExecutorService executorService;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info(asciiArt());
        int numofThreads = Integer.parseInt(properties.get("num.threads"));
        initWriter(properties);
        executorService = Executors.newFixedThreadPool(numofThreads);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.trace("Putting {} to SendGrid.", records);
        executorService.submit(()-> writer.write(records));
    }

    @Override
    public void stop() {
        
        /*if (writer != null) {
            //writer.stop();
        } */
        if(executorService != null){
            executorService.shutdown();
        }
        log.info("Stopping SendGridSinkTask.");
    }

    private void initWriter(final Map<String, String> config) {
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