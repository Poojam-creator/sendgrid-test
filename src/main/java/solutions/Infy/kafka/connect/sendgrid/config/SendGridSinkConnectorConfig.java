
package solutions.Infy.kafka.connect.sendgrid.config;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


/**
 *
 */
@Slf4j
public class SendGridSinkConnectorConfig extends AbstractConfig {

    public static final String SENDGRID_API_KEY = "sendgrid.api.key";
    public static final String SENDGRID_FROM_NAME = "sendgrid.from.name";
    public static final String SENDGRID_FROM_NAME_DEFAULT = "";
    public static final String SENDGRID_FROM_EMAIL = "sendgrid.from.email";
    private static final String SENDGRID_API_KEY_DOC = "SendGrid API Key.";
    private static final String SENDGRID_FROM_NAME_DOC = "The name of the person or company that is sending the email.";
    private static final String SENDGRID_FROM_EMAIL_DOC = "The email address of the person or company that is sending the email.";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String ACK_TOPIC = "ack.topic";
    private static final String BOOTSTRAP_SERVERS_DOC ="List of Kafka Broker servers";
    private static final String ACK_TOPIC_DOC = "The respones topic for storing api mid";
    public SendGridSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public SendGridSinkConnectorConfig(final Map<String, String> parsedConfig) {
        super(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(SENDGRID_API_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SENDGRID_API_KEY_DOC)
                .define(SENDGRID_FROM_EMAIL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SENDGRID_FROM_EMAIL_DOC)
                .define(SENDGRID_FROM_NAME, ConfigDef.Type.STRING, SENDGRID_FROM_NAME_DEFAULT, ConfigDef.Importance.LOW, SENDGRID_FROM_NAME_DOC)
                .define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(ACK_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ACK_TOPIC_DOC);
    }

}