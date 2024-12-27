
package solutions.Infy.kafka.connect.sendgrid.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solutions.Infy.kafka.connect.sendgrid.Version;
import solutions.Infy.kafka.connect.sendgrid.config.SendGridSinkConnectorConfig;

/**
 * Entry point for Kafka Connect SendGrid Sink.
 */
public class SendGridSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(SendGridSinkConnector.class);
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> properties) {
        log.info("started connector along with properties -{}", properties.toString());
        this.config = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SendGridSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // nothing to do
    }

    @Override
    public ConfigDef config() {
        return SendGridSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

}