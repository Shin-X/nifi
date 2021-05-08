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

package org.apache.nifi.controller;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.nar.OSUtil;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.test.processors.ModifiesClasspathNoAnnotationProcessor;
import org.apache.nifi.test.processors.ModifiesClasspathProcessor;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardProcessorNodeIT {

    private MockVariableRegistry variableRegistry;
    private Bundle systemBundle;
    private ExtensionDiscoveringManager extensionManager;
    private NiFiProperties niFiProperties;

    private final AtomicReference<InstanceClassLoader> currentInstanceClassLoaderHolder = new AtomicReference<>();

    @Before
    public void setup() {
        variableRegistry = new MockVariableRegistry();
        niFiProperties = NiFiProperties.createBasicNiFiProperties("src/test/resources/conf/nifi.properties");

        systemBundle = SystemBundle.create(niFiProperties);
        extensionManager = new StandardExtensionDiscoveringManager() {
            @Override
            public InstanceClassLoader createInstanceClassLoader(String classType, String instanceIdentifier, Bundle bundle, Set<URL> additionalUrls) {
                InstanceClassLoader instanceClassLoader = super.createInstanceClassLoader(classType, instanceIdentifier, bundle, additionalUrls);

                currentInstanceClassLoaderHolder.set(instanceClassLoader);

                return instanceClassLoader;

            }
        };
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
    }

    @Test(timeout = 10000)
    public void testStart() throws InterruptedException {
        final ProcessorThatThrowsExceptionOnScheduled processor = new ProcessorThatThrowsExceptionOnScheduled();
        final String uuid = UUID.randomUUID().toString();

        ProcessorInitializationContext initContext = new StandardProcessorInitializationContext(uuid, null, null, null, KerberosConfig.NOT_CONFIGURED);
        processor.initialize(initContext);

        final ReloadComponent reloadComponent = mock(ReloadComponent.class);
        final BundleCoordinate coordinate = mock(BundleCoordinate.class);

        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(processor, coordinate, null);
        final StandardProcessorNode procNode = new StandardProcessorNode(loggableComponent, uuid, createValidationContextFactory(), null, null,
            new StandardComponentVariableRegistry(VariableRegistry.EMPTY_REGISTRY), reloadComponent, extensionManager, new SynchronousValidationTrigger());
        final ScheduledExecutorService taskScheduler = new FlowEngine(1, "TestClasspathResources", true);

        final StandardProcessContext processContext = new StandardProcessContext(procNode, null, null, null, () -> false, null);
        final SchedulingAgentCallback schedulingAgentCallback = new SchedulingAgentCallback() {
            @Override
            public void onTaskComplete() {
            }

            @Override
            public Future<?> scheduleTask(final Callable<?> task) {
                return taskScheduler.submit(task);
            }

            @Override
            public void trigger() {
                Assert.fail("Should not have completed");
            }
        };

        procNode.performValidation();
        procNode.start(taskScheduler, 20000L, 10000L, () -> processContext, schedulingAgentCallback, true);

        Thread.sleep(1000L);
        assertEquals(1, processor.onScheduledCount);
        assertEquals(1, processor.onUnscheduledCount);
        assertEquals(1, processor.onStoppedCount);
    }


    @Test
    public void testSinglePropertyDynamicallyModifiesClasspath() throws MalformedURLException {
        final MockReloadComponent reloadComponent = new MockReloadComponent();

        final PropertyDescriptor classpathProp = new PropertyDescriptor.Builder().name("Classpath Resources")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp));
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(reloadComponent.getAdditionalUrls(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties of the processor to point to the test resources directory
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources");
            procNode.setProperties(properties);

            // Should have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResource));
            }

            assertEquals(ModifiesClasspathProcessor.class.getCanonicalName(), reloadComponent.getNewType());

            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }

    @Test
    public void testNativeLibLoadedFromDynamicallyModifiesClasspathProperty() throws Exception {
        // GIVEN
        assumeTrue("Test only runs on Mac OS", new OSUtil(){}.isOsMac());

        // Init NiFi
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        when(narClassLoader.getURLs()).thenReturn(new URL[0]);

        Bundle narBundle = SystemBundle.create(niFiProperties, narClassLoader);

        HashMap<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "1 sec");
        additionalProperties.put(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "target/test-classes/state-management.xml");
        additionalProperties.put(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider");
        additionalProperties.put(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        additionalProperties.put("nifi.remote.input.socket.port", "");
        additionalProperties.put("nifi.remote.input.secure", "");

        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties("src/test/resources/conf/nifi.properties", additionalProperties);

        final FlowController flowController = FlowController.createStandaloneInstance(mock(FlowFileEventRepository.class), nifiProperties,
            mock(Authorizer.class), mock(AuditService.class), null, new VolatileBulletinRepository(),
            new FileBasedVariableRegistry(nifiProperties.getVariableRegistryPropertiesPaths()),
            mock(FlowRegistryClient.class), extensionManager);

        // Init processor
        final PropertyDescriptor classpathProp = new PropertyDescriptor.Builder().name("Classpath Resources")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp));
        final String uuid = UUID.randomUUID().toString();

        final ValidationContextFactory validationContextFactory = createValidationContextFactory();
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final TerminationAwareLogger componentLog = mock(TerminationAwareLogger.class);

        final ReloadComponent reloadComponent = new StandardReloadComponent(flowController);
        ProcessorInitializationContext initContext = new StandardProcessorInitializationContext(uuid, componentLog, null, null, KerberosConfig.NOT_CONFIGURED);
        ((Processor) processor).initialize(initContext);

        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(processor, narBundle.getBundleDetails().getCoordinate(), componentLog);
        final StandardProcessorNode procNode = new StandardProcessorNode(loggableComponent, uuid, validationContextFactory, processScheduler,
            null, new StandardComponentVariableRegistry(variableRegistry), reloadComponent, extensionManager, new SynchronousValidationTrigger());

        final Map<String, String> properties = new HashMap<>();
        properties.put(classpathProp.getName(), "src/test/resources/native");
        procNode.setProperties(properties);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());

            // WHEN
            String actualLibraryLocation = currentInstanceClassLoaderHolder.get().findLibrary("testjni");

            // THEN
            assertThat(actualLibraryLocation, containsString(currentInstanceClassLoaderHolder.get().getIdentifier()));
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }


    @Test
    public void testUpdateOtherPropertyDoesNotImpactClasspath() throws MalformedURLException {
        final MockReloadComponent reloadComponent = new MockReloadComponent();

        final PropertyDescriptor classpathProp = new PropertyDescriptor.Builder().name("Classpath Resources")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final PropertyDescriptor otherProp = new PropertyDescriptor.Builder().name("My Property")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp, otherProp));
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){
            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(reloadComponent.getAdditionalUrls(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties of the processor to point to the test resources directory
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources");
            procNode.setProperties(properties);

            // Should have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResource));
            }

            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());

            // Simulate setting updating the other property which should not change the classpath
            final Map<String, String> otherProperties = new HashMap<>();
            otherProperties.put(otherProp.getName(), "foo");
            procNode.setProperties(otherProperties);

            // Should STILL have all of the resources loaded into the InstanceClassLoader now
            for (URL testResource : testResources) {
                assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResource));
            }

            // Should STILL pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());

            // Lets update the classpath property and make sure the resources get updated
            final Map<String, String> newClasspathProperties = new HashMap<>();
            newClasspathProperties.put(classpathProp.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            procNode.setProperties(newClasspathProperties);

            // Should only have resource1 loaded now
            assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResources[0]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[1]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[2]));

            assertEquals(ModifiesClasspathProcessor.class.getCanonicalName(), reloadComponent.getNewType());

            // Should STILL pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }

    @Test
    public void testMultiplePropertiesDynamicallyModifyClasspathWithExpressionLanguage() throws MalformedURLException {
        final MockReloadComponent reloadComponent = new MockReloadComponent();

        final PropertyDescriptor classpathProp1 = new PropertyDescriptor.Builder().name("Classpath Resource 1")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final PropertyDescriptor classpathProp2 = new PropertyDescriptor.Builder().name("Classpath Resource 2")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp1, classpathProp2));
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(reloadComponent.getAdditionalUrls(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties pointing to two of the resources
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp1.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            properties.put(classpathProp2.getName(), "src/test/resources/TestClasspathResources/${myResource}");

            variableRegistry.setVariable(new VariableDescriptor("myResource"), "resource3.txt");

            procNode.setProperties(properties);

            // Should have resources 1 and 3 loaded into the InstanceClassLoader now
            assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResources[0]));
            assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResources[2]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[1]));

            assertEquals(ModifiesClasspathProcessor.class.getCanonicalName(), reloadComponent.getNewType());

            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }

    @Test
    public void testSomeNonExistentPropertiesDynamicallyModifyClasspath() throws MalformedURLException {
        final MockReloadComponent reloadComponent = new MockReloadComponent();

        final PropertyDescriptor classpathProp1 = new PropertyDescriptor.Builder().name("Classpath Resource 1")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        final PropertyDescriptor classpathProp2 = new PropertyDescriptor.Builder().name("Classpath Resource 2")
            .dynamicallyModifiesClasspath(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        final ModifiesClasspathProcessor processor = new ModifiesClasspathProcessor(Arrays.asList(classpathProp1, classpathProp2));
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){

            // Should not have any of the test resources loaded at this point
            final URL[] testResources = getTestResources();
            for (URL testResource : testResources) {
                if (containsResource(reloadComponent.getAdditionalUrls(), testResource)) {
                    fail("found resource that should not have been loaded");
                }
            }

            // Simulate setting the properties pointing to two of the resources
            final Map<String, String> properties = new HashMap<>();
            properties.put(classpathProp1.getName(), "src/test/resources/TestClasspathResources/resource1.txt");
            properties.put(classpathProp2.getName(), "src/test/resources/TestClasspathResources/DoesNotExist.txt");
            procNode.setProperties(properties);

            // Should have resources 1 and 3 loaded into the InstanceClassLoader now
            assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResources[0]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[1]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[2]));

            assertEquals(ModifiesClasspathProcessor.class.getCanonicalName(), reloadComponent.getNewType());

            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }

    @Test
    public void testPropertyModifiesClasspathWhenProcessorMissingAnnotation() throws MalformedURLException {
        final MockReloadComponent reloadComponent = new MockReloadComponent();
        final ModifiesClasspathNoAnnotationProcessor processor = new ModifiesClasspathNoAnnotationProcessor();
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())){

            final Map<String, String> properties = new HashMap<>();
            properties.put(ModifiesClasspathNoAnnotationProcessor.CLASSPATH_RESOURCE.getName(),
                "src/test/resources/TestClasspathResources/resource1.txt");
            procNode.setProperties(properties);

            final URL[] testResources = getTestResources();
            assertTrue(containsResource(reloadComponent.getAdditionalUrls(), testResources[0]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[1]));
            assertFalse(containsResource(reloadComponent.getAdditionalUrls(), testResources[2]));

            assertEquals(ModifiesClasspathNoAnnotationProcessor.class.getCanonicalName(), reloadComponent.getNewType());

            // Should pass validation
            assertTrue(procNode.computeValidationErrors(procNode.getValidationContext()).isEmpty());
        } finally {
            extensionManager.removeInstanceClassLoader(procNode.getIdentifier());
        }
    }

    @Test
    public void testVerifyCanUpdateBundle() {
        final ReloadComponent reloadComponent = new MockReloadComponent();
        final ModifiesClasspathNoAnnotationProcessor processor = new ModifiesClasspathNoAnnotationProcessor();
        final StandardProcessorNode procNode = createProcessorNode(processor, reloadComponent);
        final BundleCoordinate existingCoordinate = procNode.getBundleCoordinate();

        // should be allowed to update when the bundle is the same
        procNode.verifyCanUpdateBundle(existingCoordinate);

        // should be allowed to update when the group and id are the same but version is different
        final BundleCoordinate diffVersion = new BundleCoordinate(existingCoordinate.getGroup(), existingCoordinate.getId(), "v2");
        assertTrue(!existingCoordinate.getVersion().equals(diffVersion.getVersion()));
        procNode.verifyCanUpdateBundle(diffVersion);

        // should not be allowed to update when the bundle id is different
        final BundleCoordinate diffId = new BundleCoordinate(existingCoordinate.getGroup(), "different-id", existingCoordinate.getVersion());
        assertTrue(!existingCoordinate.getId().equals(diffId.getId()));
        try {
            procNode.verifyCanUpdateBundle(diffId);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {

        }

        // should not be allowed to update when the bundle group is different
        final BundleCoordinate diffGroup = new BundleCoordinate("different-group", existingCoordinate.getId(), existingCoordinate.getVersion());
        assertTrue(!existingCoordinate.getGroup().equals(diffGroup.getGroup()));
        try {
            procNode.verifyCanUpdateBundle(diffGroup);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {

        }
    }

    private StandardProcessorNode createProcessorNode(final Processor processor, final ReloadComponent reloadComponent) {
        final String uuid = UUID.randomUUID().toString();
        final ValidationContextFactory validationContextFactory = createValidationContextFactory();
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final TerminationAwareLogger componentLog = mock(TerminationAwareLogger.class);

        extensionManager.createInstanceClassLoader(processor.getClass().getName(), uuid, systemBundle, null);

        ProcessorInitializationContext initContext = new StandardProcessorInitializationContext(uuid, componentLog, null, null, KerberosConfig.NOT_CONFIGURED);
        processor.initialize(initContext);

        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(processor, systemBundle.getBundleDetails().getCoordinate(), componentLog);
        return new StandardProcessorNode(loggableComponent, uuid, validationContextFactory, processScheduler,
            null, new StandardComponentVariableRegistry(variableRegistry), reloadComponent, extensionManager, new SynchronousValidationTrigger());
    }

    private static class MockReloadComponent implements ReloadComponent {

        private String newType;
        private final Set<URL> additionalUrls = new LinkedHashSet<>();

        public Set<URL> getAdditionalUrls() {
            return this.additionalUrls;
        }

        public String getNewType() {
            return newType;
        }

        @Override
        public void reload(ProcessorNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls) throws ProcessorInstantiationException {
            reload(newType, additionalUrls);
        }

        @Override
        public void reload(ControllerServiceNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls) throws ControllerServiceInstantiationException {
            reload(newType, additionalUrls);
        }

        @Override
        public void reload(ReportingTaskNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls) throws ReportingTaskInstantiationException {
            reload(newType, additionalUrls);
        }

        private void reload(String newType, Set<URL> additionalUrls) {
            this.newType = newType;
            this.additionalUrls.clear();
            if (additionalUrls != null) {
                this.additionalUrls.addAll(additionalUrls);
            }
        }
    }

    private boolean containsResource(Set<URL> resources, URL resourceToFind) {
        for (URL resource : resources) {
            if (resourceToFind.getPath().equals(resource.getPath())) {
                return true;
            }
        }
        return false;
    }

    private URL[] getTestResources() throws MalformedURLException {
        URL resource1 = new File("src/test/resources/TestClasspathResources/resource1.txt").toURI().toURL();
        URL resource2 = new File("src/test/resources/TestClasspathResources/resource2.txt").toURI().toURL();
        URL resource3 = new File("src/test/resources/TestClasspathResources/resource3.txt").toURI().toURL();
        return new URL[] { resource1, resource2, resource3 };
    }


    private ValidationContextFactory createValidationContextFactory() {
        return new ValidationContextFactory() {
            @Override
            public ValidationContext newValidationContext(Map<PropertyDescriptor, PropertyConfiguration> properties, String annotationData, String groupId, String componentId,
                                                          ParameterContext context) {
                return new ValidationContext() {

                    @Override
                    public ControllerServiceLookup getControllerServiceLookup() {
                        return null;
                    }

                    @Override
                    public ValidationContext getControllerServiceValidationContext(ControllerService controllerService) {
                        return null;
                    }

                    @Override
                    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
                        return null;
                    }

                    @Override
                    public PropertyValue getProperty(PropertyDescriptor property) {
                        final PropertyConfiguration configuration = properties.get(property);
                        return newPropertyValue(configuration == null ? null : configuration.getRawValue());
                    }

                    @Override
                    public PropertyValue newPropertyValue(String value) {
                        return new MockPropertyValue(value);
                    }

                    @Override
                    public Map<PropertyDescriptor, String> getProperties() {
                        final Map<PropertyDescriptor, String> propertyMap = new HashMap<>();
                        properties.forEach((k, v) -> propertyMap.put(k, v == null ? null : v.getRawValue()));
                        return propertyMap;
                    }

                    @Override
                    public Map<String, String> getAllProperties() {
                        final Map<String,String> propValueMap = new LinkedHashMap<>();
                        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
                            propValueMap.put(entry.getKey().getName(), entry.getValue());
                        }
                        return propValueMap;
                    }

                    @Override
                    public String getAnnotationData() {
                        return null;
                    }

                    @Override
                    public boolean isValidationRequired(ControllerService service) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguagePresent(String value) {
                        return false;
                    }

                    @Override
                    public boolean isExpressionLanguageSupported(String propertyName) {
                        return false;
                    }

                    @Override
                    public String getProcessGroupIdentifier() {
                        return groupId;
                    }

                    @Override
                    public Collection<String> getReferencedParameters(final String propertyName) {
                        return Collections.emptyList();
                    }

                    @Override
                    public boolean isParameterDefined(final String parameterName) {
                        return false;
                    }

                    @Override
                    public boolean isParameterSet(final String parameterName) {
                        return false;
                    }

                    @Override
                    public boolean isDependencySatisfied(final PropertyDescriptor propertyDescriptor, final Function<String, PropertyDescriptor> propertyDescriptorLookup) {
                        return false;
                    }
                };
            }

        };

    }


    public static class ProcessorThatThrowsExceptionOnScheduled extends AbstractProcessor {
        private int onScheduledCount = 0;
        private int onUnscheduledCount = 0;
        private int onStoppedCount = 0;

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

        @OnScheduled
        public void onScheduled() {
            onScheduledCount++;
            throw new ProcessException("OnScheduled called - Unit Test throws Exception intentionally");
        }

        @OnUnscheduled
        public void onUnscheduled() {
            onUnscheduledCount++;
        }

        @OnStopped
        public void onStopped() {
            onStoppedCount++;
        }
    }

}
