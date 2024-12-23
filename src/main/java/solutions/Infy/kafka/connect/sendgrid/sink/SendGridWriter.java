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
import java.util.Map;
import java.util.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 *
 */
@Slf4j
public class SendGridWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SendGrid client; // Make this re-initializable
    private Email from;
    private final KafkaProducer<String, String> producer;
    private final String ackTopic;

    public SendGridWriter(Map<String, String> properties) {
        // Default client initialization, updated dynamically in write()
        this.client = null;

        // Initialize "from" email using properties (default fallback)
        this.from = new Email(properties.get("sendgrid.from.email"), properties.get("sendgrid.from.name"));

        // Initialize Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.ackTopic = properties.get("ack.topic");
    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord sinkRecord : records) {
            String source = null;
            String transactionId = null;

            try {
                JsonNode jsonNode = MAPPER.valueToTree(sinkRecord.value());
                log.info("Processing record: {}", MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));

                source = jsonNode.get("Source").asText();
                transactionId = jsonNode.get("transactionId").asText();

                // Initialize SendGrid client dynamically using authToken
                if (jsonNode.has("authToken")) {
                    String authToken = jsonNode.get("authToken").asText();
                    this.client = new SendGrid(authToken);
                } else {
                    log.error("Missing 'authToken' in input record.");
                    continue; // Skip this record
                }

                // Construct email
                Mail mail = new Mail();
                Personalization personalization = new Personalization();

                // Set dynamic "from" email
                if (jsonNode.has("from")) {
                    String fromEmail = jsonNode.get("from").asText();
                    this.from = new Email(fromEmail);
                }
                mail.setFrom(from);

                // Set subject and content
                String subject = jsonNode.get("subject").asText();
                personalization.setSubject(subject);

                String body = jsonNode.get("emailBody").asText();
                Content content = new Content(jsonNode.get("bodyType").asText(), body);
                mail.addContent(content);

                // Add recipients
                JsonNode recipients = jsonNode.get("to");
                if (recipients.isArray()) {
                    for (final JsonNode recipient : recipients) {
                        String emailAddress = recipient.get("to").asText();
                        personalization.addTo(new Email(emailAddress));
                    }
                }
                mail.addPersonalization(personalization);

                // Create SendGrid request
                Request request = new Request();
                try {
                    request.setMethod(Method.POST);
                    request.setEndpoint("mail/send");
                    request.setBody(mail.build());

                    // Send email
                    Response response = client.api(request);
                    log.info("Response: " + response.toString());
                    log.info("Response Headers: " + response.getHeaders().toString());
                    int statusCode = response.getStatusCode();

                    if (statusCode >= 200 && statusCode < 300) { // Success
                        String messageId = response.getHeaders().get("X-Message-Id");
                        log.info("Email sent successfully. Message ID: {}", messageId);

                        // Publish success ACK to Kafka
                        String ackMessage = successACK(source, transactionId, messageId);
                        publishMessage(ackMessage);
                    } else {
                        // Log failure details
                        log.error("Failed to send email. Status Code: {}, Body: {}", statusCode, response.getBody());

                        // Publish error ACK to Kafka
                        String ackMessage = errorACK(source, transactionId, "SendGrid API Error: " + response.getBody());
                        publishMessage(ackMessage);
                    }
                } catch (IOException ex) {
                    log.error("SendGrid API communication error: {}", ex.getMessage());

                    // Publish error ACK to Kafka
                    String ackMessage = errorACK(source, transactionId, ex.getMessage());
                    publishMessage(ackMessage);
                }
            } catch (Exception e) {
                if (transactionId != null) {
                    log.error("Error while processing record for TransactionID - {}, Error: {}", transactionId, e.getMessage());
                } else {
                    log.error("Error while processing record: {}", e.getMessage());
                }
                publishMessage(errorACK(source, transactionId, e.getMessage()));
            }
        }
    }

    // Method to publish messages to Kafka
    public void publishMessage(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(ackTopic, message);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending message to Kafka", exception);
            } else {
                log.info("Message sent to Kafka topic: {}", ackTopic);
            }
        });
    }

    // Method to create success ACK message
    public String successACK(String source, String transactionId, String messageId) {
        ObjectNode ackNode = MAPPER.createObjectNode();
        ackNode.put("Source", source);
        ackNode.put("transactionId", transactionId);
        ackNode.put("messageId", messageId);
        try {
            return MAPPER.writeValueAsString(ackNode);
        } catch (JsonProcessingException e) {
            log.error("Error while creating success ACK message", e);
            return null;
        }
    }

    // Method to create error ACK message
    public String errorACK(String source, String transactionId, String errorMessage) {
        ObjectNode ackNode = MAPPER.createObjectNode();
        ackNode.put("Source", source);
        ackNode.put("transactionId", transactionId);
        ackNode.put("errorMessage", errorMessage);
        try {
            return MAPPER.writeValueAsString(ackNode);
        } catch (JsonProcessingException e) {
            log.error("Error while creating error ACK message", e);
            return null;
        }
    }

    // Method to close the Kafka producer
    public void closeProducer() {
        if (producer != null) {
            try {
                log.info("Closing Kafka producer...");
                producer.close();
            } catch (Exception e) {
                log.error("Error while closing Kafka producer", e);
            }
        }
    }
}

