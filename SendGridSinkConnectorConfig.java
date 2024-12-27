
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

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String SENDGRID_API_KEY = "sendgrid.api.key";
    public static final String SENDGRID_FROM_NAME = "sendgrid.from.name";
    public static final String SENDGRID_FROM_EMAIL = "sendgrid.from.email";
    public static final String SENDGRID_FROM_NAME_DEFAULT = "";
    public static final String RETRIES = "retries";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String ACK_TOPIC = "ack.topic";
    public static final String DLQ_TOPIC = "dlq.topic";
    public static final String RETRY_DELAY = "retry.delay";
    public static final String SECURITYPROTOCOL = "security.protocol";
    public static final String SASLMECHANISM = "sasl.mechanism";
    private static final String BOOTSTRAP_SERVERS_DOC = "bootstrap.servers";
    private static final String SENDGRID_API_KEY_DOC = "sendgrid Api Key";
    private static final String SENDGRID_FROM_NAME_DOC = "The name of the person or company that is sending the email.";
    private static final String SENDGRID_FROM_EMAIL_DOC = "The email address of the person or company that is sending the email.";
    private static final String RETRIES_DOC = "retries";
    private static final String USERNAME_DOC = "username";
    private static final String PASSWORD_DOC = "Password";
    private static final String ACK_TOPIC_DOC = "ACK Topic Name";
    private static final String DLQ_TOPIC_DOC = "DLQ Topic";
    private static final String RETRY_DELAY_DOC = "Delay in millisec for every retry";
    public static final String SECURITYPROTOCOL_DOC = "security.protocols like SASL_SSL, SASL_PLAINTEXT";
    public static final String SASLMECHANISM_DOC = "sasl mechanism";

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
                .define(SENDGRID_FROM_NAME, ConfigDef.Type.STRING, SENDGRID_FROM_NAME_DEFAULT, ConfigDef.Importance.LOW,SENDGRID_FROM_NAME_DOC)
                .define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
                .define(ACK_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ACK_TOPIC_DOC)
                .define(RETRIES,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,RETRIES_DOC)
                .define(USERNAME,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,USERNAME_DOC)
                .define(PASSWORD,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,PASSWORD_DOC)
                .define(DLQ_TOPIC,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,DLQ_TOPIC_DOC)
                .define(RETRY_DELAY,ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,RETRY_DELAY_DOC)
                .define(SECURITYPROTOCOL,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,SECURITYPROTOCOL_DOC)
                .define(SASLMECHANISM,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,SASLMECHANISM_DOC);
    }

}