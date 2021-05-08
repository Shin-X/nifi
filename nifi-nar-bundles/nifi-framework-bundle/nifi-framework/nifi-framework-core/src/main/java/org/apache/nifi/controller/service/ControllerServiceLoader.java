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
package org.apache.nifi.controller.service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ControllerServiceLoader {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceLoader.class);

    public static List<ControllerServiceNode> loadControllerServices(final FlowController controller, final InputStream serializedStream, final ProcessGroup parentGroup,
        final StringEncryptor encryptor, final BulletinRepository bulletinRepo, final boolean autoResumeState, final FlowEncodingVersion encodingVersion) throws IOException {

        try (final InputStream in = new BufferedInputStream(serializedStream)) {
            final DocumentBuilder builder = XmlUtils.createSafeDocumentBuilder(null);

            builder.setErrorHandler(new org.xml.sax.ErrorHandler() {

                @Override
                public void fatalError(final SAXParseException err) throws SAXException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void error(final SAXParseException err) throws SAXParseException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void warning(final SAXParseException err) throws SAXParseException {
                    logger.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.warn("Warning stack dump", err);
                    }
                    throw err;
                }
            });

            final Document document = builder.parse(in);
            final Element controllerServices = document.getDocumentElement();
            final List<Element> serviceElements = DomUtils.getChildElementsByTagName(controllerServices, "controllerService");

            final Map<ControllerServiceNode, Element> controllerServiceMap = ControllerServiceLoader.loadControllerServices(serviceElements, controller, parentGroup, encryptor, encodingVersion);
            enableControllerServices(controllerServiceMap, controller, encryptor, autoResumeState, encodingVersion);
            return new ArrayList<>(controllerServiceMap.keySet());
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        }
    }

    public static Map<ControllerServiceNode, Element> loadControllerServices(final List<Element> serviceElements, final FlowController controller,
                                                                             final ProcessGroup parentGroup, final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

        final Map<ControllerServiceNode, Element> nodeMap = new HashMap<>();
        for (final Element serviceElement : serviceElements) {
            final ControllerServiceNode serviceNode = createControllerService(controller, serviceElement, encryptor, encodingVersion);
            if (parentGroup == null) {
                controller.getFlowManager().addRootControllerService(serviceNode);
            } else {
                parentGroup.addControllerService(serviceNode);
            }

            // We need to clone the node because it will be used in a separate thread below, and
            // Element is not thread-safe.
            nodeMap.put(serviceNode, (Element) serviceElement.cloneNode(true));
        }
        for (final Map.Entry<ControllerServiceNode, Element> entry : nodeMap.entrySet()) {
            configureControllerService(entry.getKey(), entry.getValue(), encryptor, encodingVersion);
        }

        return nodeMap;
    }

    public static void enableControllerServices(final Map<ControllerServiceNode, Element> nodeMap, final FlowController controller,
                                                final StringEncryptor encryptor, final boolean autoResumeState, final FlowEncodingVersion encodingVersion) {
        // Start services
        if (autoResumeState) {
            final Set<ControllerServiceNode> nodesToEnable = new HashSet<>();

            for (final ControllerServiceNode node : nodeMap.keySet()) {
                final Element controllerServiceElement = nodeMap.get(node);

                final ControllerServiceDTO dto;
                synchronized (controllerServiceElement.getOwnerDocument()) {
                    dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);
                }

                final ControllerServiceState state = ControllerServiceState.valueOf(dto.getState());
                if (state == ControllerServiceState.ENABLED) {
                    nodesToEnable.add(node);
                    logger.debug("Will enable Controller Service {}", node);
                } else {
                    logger.debug("Will not enable Controller Service {} because its state is set to {}", node, state);
                }
            }

            enableControllerServices(nodesToEnable, controller, autoResumeState);
        } else {
            logger.debug("Will not enable the following Controller Services because 'auto-resume state' flag is false: {}", nodeMap.keySet());
        }
    }

    public static void enableControllerServices(final Collection<ControllerServiceNode> nodesToEnable, final FlowController controller, final boolean autoResumeState) {
        // Start services
        if (autoResumeState) {
            logger.debug("Enabling Controller Services {}", nodesToEnable);
            nodesToEnable.forEach(ControllerServiceNode::performValidation); // validate services before attempting to enable them

            controller.getControllerServiceProvider().enableControllerServices(nodesToEnable);
        } else {
            logger.debug("Will not enable the following Controller Services because 'auto-resume state' flag is false: {}", nodesToEnable);
        }
    }

    public static ControllerServiceNode cloneControllerService(final FlowController flowController, final ControllerServiceNode controllerService) {
        // create a new id for the clone seeded from the original id so that it is consistent in a cluster
        final UUID id = UUID.nameUUIDFromBytes(controllerService.getIdentifier().getBytes(StandardCharsets.UTF_8));

        final ControllerServiceNode clone = flowController.getFlowManager().createControllerService(controllerService.getCanonicalClassName(), id.toString(),
                controllerService.getBundleCoordinate(), Collections.emptySet(), false, true);
        clone.setName(controllerService.getName());
        clone.setComments(controllerService.getComments());

        if (controllerService.getProperties() != null) {
            Map<String,String> properties = new HashMap<>();
            for (Map.Entry<PropertyDescriptor, String> propEntry : controllerService.getRawPropertyValues().entrySet()) {
                properties.put(propEntry.getKey().getName(), propEntry.getValue());
            }
            clone.setProperties(properties);
        }

        return clone;
    }

    private static ControllerServiceNode createControllerService(final FlowController flowController, final Element controllerServiceElement, final StringEncryptor encryptor,
                                                                 final FlowEncodingVersion encodingVersion) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);

        BundleCoordinate coordinate;
        try {
            coordinate = BundleUtils.getCompatibleBundle(flowController.getExtensionManager(), dto.getType(), dto.getBundle());
        } catch (final IllegalStateException e) {
            final BundleDTO bundleDTO = dto.getBundle();
            if (bundleDTO == null) {
                coordinate = BundleCoordinate.UNKNOWN_COORDINATE;
            } else {
                coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
            }
        }

        final ControllerServiceNode node = flowController.getFlowManager().createControllerService(dto.getType(), dto.getId(), coordinate, Collections.emptySet(), false, true);
        node.setName(dto.getName());
        node.setComments(dto.getComments());
        node.setVersionedComponentId(dto.getVersionedComponentId());
        return node;
    }

    private static void configureControllerService(final ControllerServiceNode node, final Element controllerServiceElement, final StringEncryptor encryptor,
                                                   final FlowEncodingVersion encodingVersion) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);
        node.pauseValidationTrigger();
        try {
            node.setAnnotationData(dto.getAnnotationData());
            node.setProperties(dto.getProperties());
        } finally {
            node.resumeValidationTrigger();
        }
    }

}
