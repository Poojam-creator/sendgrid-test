/*
 * Copyright 2019 SMB GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package solutions.Infy.kafka.connect.sendgrid.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;
import com.sendgrid.helpers.mail.Mail;
import com.sendgrid.helpers.mail.objects.Content;
import com.sendgrid.helpers.mail.objects.Email;
import com.sendgrid.helpers.mail.objects.Personalization;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solutions.Infy.kafka.connect.sendgrid.config.SendGridSinkConnectorConfig;

/**
 *
 */
public class SendGridWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectMapper MAPPER1 = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(SendGridWriter.class);
    private static final int BACKOFF_MULTIPLIER = 2;
    private static int MAX_RETRIES;

    private KafkaProducer<String, String> producer;
    private SendGrid client; // Make this re-initializable
    private Email from;
    private final SendGridSinkConnectorConfig config;
    public final String ackTopic;

    public SendGridWriter(Map<String, String> properties) {
        config = new SendGridSinkConnectorConfig(properties);
        MAX_RETRIES = Integer.parseInt(config.getString(SendGridSinkConnectorConfig.RETRIES));
    //}
        // Initialize Kafka producer properties
        //Properties props = new Properties();
        //producer = new KafkaProducer<>(props);
        /*props.put("bootstrap.servers",config.getString(SendGridSinkConnectorConfig.BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "CustomSinkProducer");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.SECURITY_PROVIDERS_CONFIG, config.getString(SendGridSinkConnectorConfig.SECURITYPROTOCOL));
        props.put("sasl.mechanism", config.getString(SendGridSinkConnectorConfig.SASLMECHANISM));
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='"+config.getString(SendGridSinkConnectorConfig.USERNAME)+"' password='"+config.getString(SendGridSinkConnectorConfig.PASSWORD)+"';");
        log.info("Publishing message to topic for Producer ID - {}", props.getProperty("client.id"));*/

        //producer = new KafkaProducer<>(props);

        // Initialize "from" email
        this.from = new Email(config.getString(SendGridSinkConnectorConfig.SENDGRID_FROM_EMAIL),
                config.getString(SendGridSinkConnectorConfig.SENDGRID_FROM_NAME));

        this.ackTopic = config.getString(SendGridSinkConnectorConfig.ACK_TOPIC);
    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord sinkRecord : records) {
            log.info("Processing record: {}", sinkRecord.toString());
            log.info("MetaData details: key -{} ,Value-{},Kafka Partition - {}",sinkRecord.key(),sinkRecord.key(),sinkRecord.kafkaPartition());
            String source;
            String transactionId = null;

            try {
                JsonNode jsonNode = MAPPER.valueToTree(sinkRecord.value());

                if (jsonNode.get("authToken") == null || jsonNode.get("authToken").asText().isEmpty()) {
                    throw new IllegalArgumentException("authToken is mandatory and cannot be null or empty");
                }

                if (jsonNode.get("from") == null || jsonNode.get("from").asText().isEmpty()) {
                    throw new IllegalArgumentException("from email address is mandatory and cannot be null or empty");
                }

                if (jsonNode.get("subject") == null || jsonNode.get("subject").asText().isEmpty()) {
                    throw new IllegalArgumentException("subject is mandatory and cannot be null or empty");
                }

                if (jsonNode.get("emailBody") == null || jsonNode.get("emailBody").asText().isEmpty()) {
                    throw new IllegalArgumentException("emailBody is mandatory and cannot be null or empty");
                }

                source = jsonNode.get("source").asText();
                transactionId = jsonNode.get("transactionId").asText();
                log.info("Source - {} and TransactionId -{}", source, transactionId);

                // Initialize SendGrid client dynamically
                this.client = new SendGrid(jsonNode.get("authToken").asText());
                log.info("SendGrid connection initiated");

                // Construct email
                Mail mail = new Mail();
                Personalization personalization = new Personalization();

                // Set dynamic "from" email
                String fromEmail = jsonNode.get("from").asText();
                this.from = new Email(fromEmail);
                mail.setFrom(from);

                String subject = jsonNode.get("subject").asText();
                personalization.setSubject(subject);

                String body = jsonNode.get("emailBody").asText();
                Content content = new Content(jsonNode.get("bodyType").asText(), body);
                mail.addContent(content);
                log.info(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));
                JsonNode recipients = jsonNode.get("to");
                if (recipients.isArray()) {
                    for (final JsonNode recipient : recipients) {
                        String emailAddress = recipient.get("to").asText();
                        if (emailAddress == null || emailAddress.isEmpty()) {
                            throw new IllegalArgumentException("Recipient email address cannot be null or empty");
                        }
                        personalization.addTo(new Email(emailAddress));
                    }
                }

                mail.addPersonalization(personalization);

                // Call sendEmailWithRetry method
                Response response = sendEmailWithRetry(mail, transactionId,sinkRecord);

                if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                    //log.info("response" +response);
                    log.info("Email sent successfully for Transaction ID: {}", transactionId);
                    String messageId = response.getHeaders().get("X-Message-Id");
                    publishMessage(config.getString(SendGridSinkConnectorConfig.ACK_TOPIC), successACK(source, transactionId, messageId, response.getStatusCode()));
                } else {
                    log.error("Failed to send email. Status Code: {}, Body: {}", response.getStatusCode(), response.getBody());
                    throw new IOException("SendGrid API Error: " + response.getBody());
                }
            } catch (Exception e) {
                log.error("Error while processing record for TransactionID - {}: {}", transactionId, e.getMessage());
                sendToDlq(sinkRecord, e);
            }
        }
    }

    private Response sendEmailWithRetry(Mail mail, String transactionId,SinkRecord sinkRecord) {
        int retryCount = 0;
        boolean messageSent = false;
        int backoff = Integer.parseInt(config.getString(SendGridSinkConnectorConfig.RETRY_DELAY));
        //JsonNode jsonNode= MAPPER.valueToTree(sinkRecord.value());
        Response response = null;

        while (retryCount < MAX_RETRIES && !messageSent) {
            try {
                Thread.sleep(backoff);
                log.info("Retrying email send. Attempt: {}", retryCount + 1);

                // Create SendGrid request
                Request request = new Request();
                request.setMethod(Method.POST);
                request.setEndpoint("mail/send");
                request.setBody(mail.build());

                // Send email
                response = client.api(request);
                log.info("Response: " + response.toString());
                log.info("Response Headers: " + response.getHeaders().toString());
                log.info("statuscode:"+ response.getStatusCode());
                if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                    messageSent = true;
                }
            } catch (Exception e) {
                retryCount++;
                backoff *= BACKOFF_MULTIPLIER;
                log.error("Error during email send retry: {}", e.getMessage());
            }
        }

        if (!messageSent) {
            log.error("Max retries exceeded for email sending. Moving to DLQ. Transaction ID: {}", transactionId);
            sendToDlq(sinkRecord, new Exception("Message retry exceeded max retries"));
        }


        return response;
    }

    public void publishMessage(String ackTopic,String message) {
        Properties props = new Properties();
        //producer = new KafkaProducer<>(props);
        log.info("Publisher properties initialized");
        props.put("bootstrap.servers",config.getString(SendGridSinkConnectorConfig.BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "CustomSinkProducer");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(SendGridSinkConnectorConfig.SECURITYPROTOCOL));
        props.put("sasl.mechanism", config.getString(SendGridSinkConnectorConfig.SASLMECHANISM));
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='"+config.getString(SendGridSinkConnectorConfig.USERNAME)+"' password='"+config.getString(SendGridSinkConnectorConfig.PASSWORD)+"';");
        log.info("Publishing message to topic for Producer ID - {}", props.getProperty("client.id").toString());
        producer = new KafkaProducer<>(props);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(ackTopic, message);
            producer.send(record);
            log.info("Published message to topic -{}",ackTopic);
            producer.flush();
            producer.close();
        }catch (Exception e){
                log.error("Error sending message to Kafka topic- {}", e.getMessage());
            producer.close();
            }
    }

    public void sendToDlq(SinkRecord record, Exception exception) {
        try {
            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("Origin_Topic", record.topic());
            dlqMessage.put("origin_partition", record.kafkaPartition());
            dlqMessage.put("origin_offset", record.kafkaOffset());
            dlqMessage.put("error_type", exception.getClass().getSimpleName());
            dlqMessage.put("error_message", exception.getMessage());
            dlqMessage.put("original_message", record.value());
            String jsonMessage = MAPPER1.writeValueAsString(dlqMessage);
            log.info("Sending DLQ message: {}", MAPPER1.writeValueAsString(dlqMessage));
            publishMessage(config.getString(SendGridSinkConnectorConfig.DLQ_TOPIC), jsonMessage);
        } catch (JsonProcessingException e) {
            log.error("Error while generating DLQ message: {}", e.getMessage());
        }
    }

    public String successACK(String source, String transactionId, String messageId, int status) {
        ObjectNode ackNode = MAPPER.createObjectNode();
        ackNode.put("source", source);
        ackNode.put("transactionId", transactionId);
        ackNode.put("messageId", messageId);
        ackNode.put("status", status);
        try {
            return MAPPER.writeValueAsString(ackNode);
        } catch (JsonProcessingException e) {
            log.error("Error while generating success ACK: {}", e.getMessage());
            return null;
        }
    }
}

