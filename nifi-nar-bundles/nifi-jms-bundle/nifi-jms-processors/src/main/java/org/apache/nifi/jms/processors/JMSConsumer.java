/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.jms.processors;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.nifi.jms.processors.MessageBodyToBytesConverter.MessageConversionException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.jms.support.JmsUtils;

/**
 * Generic consumer of messages from JMS compliant messaging system.
 */
final class JMSConsumer extends JMSWorker {

    JMSConsumer(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ComponentLog logger) {
        super(connectionFactory, jmsTemplate, logger);
        logger.debug("Created Message Consumer for '{}'", new Object[] {jmsTemplate});
    }


    private MessageConsumer createMessageConsumer(final Session session, final String destinationName, final boolean durable, final boolean shared, final String subscriptionName,
                                                  final String messageSelector) throws JMSException {
        final boolean isPubSub = JMSConsumer.this.jmsTemplate.isPubSubDomain();
        final Destination destination = JMSConsumer.this.jmsTemplate.getDestinationResolver().resolveDestinationName(session, destinationName, isPubSub);

        if (isPubSub) {
            if (shared) {
                try {
                    if (durable) {
                        return session.createSharedDurableConsumer((Topic) destination, subscriptionName, messageSelector);
                    } else {
                        return session.createSharedConsumer((Topic) destination, subscriptionName, messageSelector);
                    }
                } catch (AbstractMethodError e) {
                    throw new ProcessException("Failed to create a shared consumer. Make sure the target broker is JMS 2.0 compliant.", e);
                }
            } else {
                if (durable) {
                    return session.createDurableConsumer((Topic) destination, subscriptionName, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                } else {
                    return session.createConsumer(destination, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
                }
            }
        } else {
            return session.createConsumer(destination, messageSelector, JMSConsumer.this.jmsTemplate.isPubSubDomain());
        }
    }


    public void consume(final String destinationName, String errorQueueName, final boolean durable, final boolean shared, final String subscriptionName, final String messageSelector,
                        final String charset, final ConsumerCallback consumerCallback) {
        this.jmsTemplate.execute(new SessionCallback<Void>() {
            @Override
            public Void doInJms(final Session session) throws JMSException {

                final MessageConsumer msgConsumer = createMessageConsumer(session, destinationName, durable, shared, subscriptionName, messageSelector);
                try {
                    final Message message = msgConsumer.receive(JMSConsumer.this.jmsTemplate.getReceiveTimeout());
                    JMSResponse response = null;

                    if (message != null) {
                        String messageType;
                        byte[] messageBody;

                        try {
                            if (message instanceof TextMessage) {
                                messageType = TextMessage.class.getSimpleName();
                                messageBody = MessageBodyToBytesConverter.toBytes((TextMessage) message, Charset.forName(charset));
                            } else if (message instanceof BytesMessage) {
                                messageType = BytesMessage.class.getSimpleName();
                                messageBody = MessageBodyToBytesConverter.toBytes((BytesMessage) message);
                            } else if (message instanceof ObjectMessage) {
                                messageType = ObjectMessage.class.getSimpleName();
                                messageBody = MessageBodyToBytesConverter.toBytes((ObjectMessage) message);
                            } else if (message instanceof StreamMessage) {
                                messageType = StreamMessage.class.getSimpleName();
                                messageBody = MessageBodyToBytesConverter.toBytes((StreamMessage) message);
                            } else if (message instanceof MapMessage) {
                                messageType = MapMessage.class.getSimpleName();
                                messageBody = MessageBodyToBytesConverter.toBytes((MapMessage) message);
                            } else {
                                acknowledge(message, session);

                                if (errorQueueName != null) {
                                    processLog.error("Received unsupported JMS Message type [{}]; rerouting message to error queue [{}].", new Object[] {message, errorQueueName});
                                    jmsTemplate.send(errorQueueName, __ -> message);
                                } else {
                                    processLog.error("Received unsupported JMS Message type [{}]; will skip this message.", new Object[] {message});
                                }

                                return null;
                            }
                        } catch (final MessageConversionException mce) {
                            processLog.error("Received a JMS Message [{}] but failed to obtain the content of the message; will acknowledge this message without creating a FlowFile for it.",
                                new Object[] {message}, mce);
                            acknowledge(message, session);

                            if (errorQueueName != null) {
                                jmsTemplate.send(errorQueueName, __ -> message);
                            }

                            return null;
                        }

                        final Map<String, String> messageHeaders = extractMessageHeaders(message);
                        final Map<String, String> messageProperties = extractMessageProperties(message);
                        response = new JMSResponse(messageType, messageBody, messageHeaders, messageProperties);
                    }

                    // invoke the processor callback (regardless if it's null,
                    // so the processor can yield) as part of this inJMS call
                    // and ACK message *only* after its successful invocation
                    // and if CLIENT_ACKNOWLEDGE is set.
                    consumerCallback.accept(response);
                    acknowledge(message, session);
                } catch (Exception e) {
                    // We need to call recover to ensure that in the event of
                    // abrupt end or exception the current session will stop message
                    // delivery and restart with the oldest unacknowledged message
                    try {
                        session.recover();
                    } catch (Exception e1) {
                        // likely the session is closed...need to catch this so that the root cause of failure is propagated
                        processLog.debug("Failed to recover JMS session while handling initial error. The recover error is: ", e1);
                    }
                    throw e;
                } finally {
                    JmsUtils.closeMessageConsumer(msgConsumer);
                }

                return null;
            }
        }, true);
    }

    private void acknowledge(final Message message, final Session session) throws JMSException {
        if (message != null && session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
            message.acknowledge();
        }
    }


    @SuppressWarnings("unchecked")
    private Map<String, String> extractMessageProperties(final Message message) {
        final Map<String, String> properties = new HashMap<>();
        try {
            final Enumeration<String> propertyNames = message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement();
                properties.put(propertyName, String.valueOf(message.getObjectProperty(propertyName)));
            }
        } catch (JMSException e) {
            this.processLog.warn("Failed to extract message properties", e);
        }
        return properties;
    }


    private Map<String, String> extractMessageHeaders(final Message message) throws JMSException {
        final Map<String, String> messageHeaders = new HashMap<>();

        messageHeaders.put(JmsHeaders.DELIVERY_MODE, String.valueOf(message.getJMSDeliveryMode()));
        messageHeaders.put(JmsHeaders.EXPIRATION, String.valueOf(message.getJMSExpiration()));
        messageHeaders.put(JmsHeaders.PRIORITY, String.valueOf(message.getJMSPriority()));
        messageHeaders.put(JmsHeaders.REDELIVERED, String.valueOf(message.getJMSRedelivered()));
        messageHeaders.put(JmsHeaders.TIMESTAMP, String.valueOf(message.getJMSTimestamp()));
        messageHeaders.put(JmsHeaders.CORRELATION_ID, message.getJMSCorrelationID());
        messageHeaders.put(JmsHeaders.MESSAGE_ID, message.getJMSMessageID());
        messageHeaders.put(JmsHeaders.TYPE, message.getJMSType());

        String replyToDestinationName = this.retrieveDestinationName(message.getJMSReplyTo(), JmsHeaders.REPLY_TO);
        if (replyToDestinationName != null) {
            messageHeaders.put(JmsHeaders.REPLY_TO, replyToDestinationName);
        }

        String destinationName = this.retrieveDestinationName(message.getJMSDestination(), JmsHeaders.DESTINATION);
        if (destinationName != null) {
            messageHeaders.put(JmsHeaders.DESTINATION, destinationName);
        }

        return messageHeaders;
    }


    private String retrieveDestinationName(Destination destination, String headerName) {
        String destinationName = null;
        if (destination != null) {
            try {
                destinationName = (destination instanceof Queue) ? ((Queue) destination).getQueueName()
                        : ((Topic) destination).getTopicName();
            } catch (JMSException e) {
                this.processLog.warn("Failed to retrieve Destination name for '" + headerName + "' header", e);
            }
        }
        return destinationName;
    }


    static class JMSResponse {
        private final byte[] messageBody;

        private final String messageType;
        private final Map<String, String> messageHeaders;
        private final Map<String, String> messageProperties;

        JMSResponse(String messageType, byte[] messageBody, Map<String, String> messageHeaders, Map<String, String> messageProperties) {
            this.messageType = messageType;
            this.messageBody = messageBody;
            this.messageHeaders = Collections.unmodifiableMap(messageHeaders);
            this.messageProperties = Collections.unmodifiableMap(messageProperties);
        }

        public String getMessageType() {
            return messageType;
        }

        public byte[] getMessageBody() {
            return this.messageBody;
        }

        public Map<String, String> getMessageHeaders() {
            return this.messageHeaders;
        }

        public Map<String, String> getMessageProperties() {
            return messageProperties;
        }
    }

    /**
     * Callback to be invoked while executing inJMS call (the call within the
     * live JMS session)
     */
    static interface ConsumerCallback {
        void accept(JMSResponse response);
    }
}
