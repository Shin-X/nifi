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

package org.apache.nifi.processors.script;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "transform", "script", "groovy", "jython", "python", "update", "modify", "filter"})
@Restricted(restrictions = {
    @Restriction(requiredPermission = RequiredPermission.EXECUTE_CODE,
        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
    @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@CapabilityDescription("提供针对传入流文件中的每个记录评估简单脚本的能力。脚本可以以某种方式转换记录，过滤记录，或者fork额外的记录。有关更多信息，请参阅处理器的附加细节。")
@SeeAlso(classNames = {"org.apache.nifi.processors.script.ExecuteScript",
    "org.apache.nifi.processors.standard.UpdateRecord",
    "org.apache.nifi.processors.standard.QueryRecord",
    "org.apache.nifi.processors.jolt.record.JoltTransformRecord",
    "org.apache.nifi.processors.standard.LookupRecord"})
public class ScriptedTransformRecord extends AbstractProcessor implements Searchable {
    private static final String PYTHON_SCRIPT_LANGUAGE = "python";
    private static final Set<String> SCRIPT_OPTIONS = ScriptingComponentUtils.getAvailableEngines();

    static final PropertyDescriptor RECORD_READER = new Builder()
        .name("Record Reader")
        .displayName("Record Reader")
        .description("The Record Reader to use parsing the incoming FlowFile into Records")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new Builder()
        .name("Record Writer")
        .displayName("Record Writer")
        .description("The Record Writer to use for serializing Records after they have been transformed")
        .required(true)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();
    static final PropertyDescriptor LANGUAGE = new Builder()
        .name("Script Engine")
        .displayName("Script Language")
        .description("The Language to use for the script")
        .allowableValues(SCRIPT_OPTIONS)
        .defaultValue("Groovy")
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Each FlowFile that were successfully transformed will be routed to this Relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be transformed will be routed to this Relationship")
        .build();


    private volatile String scriptToRun = null;
    private final AtomicReference<CompiledScript> compiledScriptRef = new AtomicReference<>();
    private final ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();
    private final List<PropertyDescriptor> descriptors = Arrays.asList(
        RECORD_READER,
        RECORD_WRITER,
        LANGUAGE,
        ScriptingComponentUtils.SCRIPT_BODY,
        ScriptingComponentUtils.SCRIPT_FILE,
        ScriptingComponentUtils.MODULES);


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return scriptingComponentHelper.customValidate(validationContext);
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        if (!scriptingComponentHelper.isInitialized.get()) {
            scriptingComponentHelper.createResources();
        }

        scriptingComponentHelper.setupVariables(context);

        // Create a script engine for each possible task
        final int maxTasks = context.getMaxConcurrentTasks();
        scriptingComponentHelper.setup(maxTasks, getLogger());
        scriptToRun = scriptingComponentHelper.getScriptBody();

        if (scriptToRun == null && scriptingComponentHelper.getScriptPath() != null) {
            try (final FileInputStream scriptStream = new FileInputStream(scriptingComponentHelper.getScriptPath())) {
                scriptToRun = IOUtils.toString(scriptStream, Charset.defaultCharset());
            }
        }
        // Always compile when first run
        compiledScriptRef.set(null);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ScriptEngine scriptEngine = scriptingComponentHelper.engineQ.poll();
        if (scriptEngine == null) {
            // This shouldn't happen. But just in case.
            session.rollback();
            return;
        }

        try {
            final ScriptEvaluator evaluator;
            try {
                evaluator = createEvaluator(scriptEngine, flowFile);
            } catch (final ScriptException se) {
                getLogger().error("Failed to initialize script engine", se);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            transform(flowFile, evaluator, context, session);
        } finally {
            scriptingComponentHelper.engineQ.offer(scriptEngine);
        }
    }

    private void transform(final FlowFile flowFile, final ScriptEvaluator evaluator, final ProcessContext context, final ProcessSession session) {
        final long startMillis = System.currentTimeMillis();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Counts counts = new Counts();
        try {
            final Map<String, String> attributesToAdd = new HashMap<>();

            // Read each record, transform it, and write out the transformed version
            session.write(flowFile, (in, out) -> {
                final AtomicReference<RecordSetWriter> writerReference = new AtomicReference<>();

                try (final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                    // We want to lazily create the Record Writer from the Record Writer Factory.
                    // We do this because if the script adds new fields to the Records, we want to ensure that the Record Writer is
                    // created with the appropriate schema that has those fields accounted for. We can only do this if we first transform the Record.
                    // By lazily creating the Writer this way, we can incorporate any newly added fields to the first Record and then write it out.
                    // Note that this means that any newly added field must be added to the first Record, even if the value is null. Otherwise, the field
                    // will not make its way to the Record Writer's schema.
                    final RecordWriteAction writeAction = new RecordWriteAction() {
                        private RecordSetWriter writer = null;

                        @Override
                        public void write(final Record record) throws IOException {
                            if (record == null) {
                                return;
                            }

                            record.incorporateInactiveFields();

                            if (writer == null) {
                                final RecordSchema writerSchema;
                                writerSchema = record.getSchema();

                                try {
                                    writer = writerFactory.createWriter(getLogger(), writerSchema, out, flowFile);
                                } catch (SchemaNotFoundException e) {
                                    throw new IOException(e);
                                }

                                writerReference.set(writer);
                                writer.beginRecordSet();
                            }

                            writer.write(record);
                        }
                    };

                    final WriteResult writeResult;
                    try {
                        // Transform each Record.
                        Record inputRecord;
                        while ((inputRecord = reader.nextRecord()) != null) {
                            processRecord(inputRecord, flowFile, counts, writeAction, evaluator);
                        }

                        // If there were no records written, we still want to create a Record Writer. We do this for two reasons.
                        // Firstly, the beginning/ending of the Record Set may result in output being written.
                        // Secondly, we obtain important attributes from the WriteResult.
                        RecordSetWriter writer = writerReference.get();
                        if (writer == null) {
                            writer = writerFactory.createWriter(getLogger(), reader.getSchema(), out, flowFile);
                            writer.beginRecordSet();
                            writeResult = writer.finishRecordSet();
                            attributesToAdd.put("mime.type", writer.getMimeType());
                        } else {
                            writeResult = writer.finishRecordSet();
                            attributesToAdd.put("mime.type", writer.getMimeType());
                        }
                    } finally {
                        final RecordSetWriter writer = writerReference.get();
                        if (writer != null) {
                            writer.close();
                        }
                    }

                    // Add WriteResults to the attributes to be added to the FlowFile
                    attributesToAdd.putAll(writeResult.getAttributes());
                    attributesToAdd.put("record.count", String.valueOf(writeResult.getRecordCount()));
                } catch (final MalformedRecordException | SchemaNotFoundException | ScriptException e) {
                    throw new ProcessException(e);
                }
            });

            // Add the necessary attributes to the FlowFile and transfer to success
            session.putAllAttributes(flowFile, attributesToAdd);
            session.transfer(flowFile, REL_SUCCESS);

            final long transformCount = counts.getRecordCount() - counts.getDroppedCount();
            getLogger().info("Successfully transformed {} Records and dropped {} Records for {}", new Object[] {transformCount, counts.getDroppedCount(), flowFile});
            session.adjustCounter("Records Transformed", transformCount, true);
            session.adjustCounter("Records Dropped", counts.getDroppedCount(), true);

            final long millis = System.currentTimeMillis() - startMillis;
            session.getProvenanceReporter().modifyContent(flowFile, "Transformed " + transformCount + " Records, Dropped " + counts.getDroppedCount() + " Records", millis);
        } catch (final ProcessException e) {
            getLogger().error("After processing {} Records, encountered failure when attempting to transform {}", new Object[] {counts.getRecordCount(), flowFile}, e.getCause());
            session.transfer(flowFile, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("After processing {} Records, encountered failure when attempting to transform {}", new Object[] {counts.getRecordCount(), flowFile}, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void processRecord(final Record inputRecord, final FlowFile flowFile, final Counts counts, final RecordWriteAction recordWriteAction,
                               final ScriptEvaluator evaluator) throws IOException, ScriptException {
        final long index = counts.getRecordCount();

        // Evaluate the script against the Record
        final Object returnValue = evaluator.evaluate(inputRecord, index);

        counts.incrementRecordCount();

        // If a null value was returned, drop the Record
        if (returnValue == null) {
            getLogger().trace("Script returned null for Record {} [{}] so will drop Record from {}", new Object[]{index, inputRecord, flowFile});
            counts.incrementDroppedCount();
            return;
        }

        // If a single Record was returned, write it out
        if (returnValue instanceof Record) {
            final Record transformedRecord = (Record) returnValue;
            getLogger().trace("Successfully transformed Record {} from {} to {} for {}", new Object[]{index, inputRecord, transformedRecord, flowFile});
            recordWriteAction.write(transformedRecord);
            return;
        }

        // If a Collection was returned, ensure that every element in the collection is a Record and write them out
        if (returnValue instanceof Collection) {
            final Collection<?> collection = (Collection<?>) returnValue;
            getLogger().trace("Successfully transformed Record {} from {} to {} for {}", new Object[]{index, inputRecord, collection, flowFile});

            for (final Object element : collection) {
                if (!(element instanceof Record)) {
                    throw new RuntimeException("Evaluated script against Record number " + index + " of " + flowFile
                        + " but instead of returning a Record or Collection of Records, script returned a Collection of values, "
                        + "at least one of which was not a Record but instead was: " + returnValue);
                }

                recordWriteAction.write((Record) element);
            }

            return;
        }

        // Ensure that the value returned from the script is either null or a Record
        throw new RuntimeException("Evaluated script against Record number " + index + " of " + flowFile
            + " but instead of returning a Record, script returned a value of: " + returnValue);
    }

    private ScriptEvaluator createEvaluator(final ScriptEngine scriptEngine, final FlowFile flowFile) throws ScriptException {
        if (PYTHON_SCRIPT_LANGUAGE.equalsIgnoreCase(scriptEngine.getFactory().getLanguageName())) {
            final CompiledScript compiledScript = getOrCompileScript((Compilable) scriptEngine, scriptToRun);
            return new PythonScriptEvaluator(scriptEngine, compiledScript, flowFile);
        }

        return new InterpretedScriptEvaluator(scriptEngine, scriptToRun, flowFile);
    }

    private CompiledScript getOrCompileScript(final Compilable scriptEngine, final String scriptToRun) throws ScriptException {
        final CompiledScript existing = compiledScriptRef.get();
        if (existing != null) {
            return existing;
        }

        final CompiledScript compiled = scriptEngine.compile(scriptToRun);
        final boolean updated = compiledScriptRef.compareAndSet(null, compiled);
        if (updated) {
            return compiled;
        }

        return compiledScriptRef.get();
    }

    private static Bindings setupBindings(final ScriptEngine scriptEngine) {
        Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
        if (bindings == null) {
            bindings = new SimpleBindings();
        }

        scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        return bindings;
    }


    @Override
    public Collection<SearchResult> search(final SearchContext context) {
        return ScriptingComponentUtils.search(context, getLogger());
    }

    private interface ScriptEvaluator {
        Object evaluate(Record record, long index) throws ScriptException;
    }


    private class PythonScriptEvaluator implements ScriptEvaluator {
        private final ScriptEngine scriptEngine;
        private final CompiledScript compiledScript;
        private final Bindings bindings;

        public PythonScriptEvaluator(final ScriptEngine scriptEngine, final CompiledScript compiledScript, final FlowFile flowFile) {
            // By pre-compiling the script here, we get significant performance gains. A quick 5-minute benchmark
            // shows gains of about 100x better performance. But even with the compiled script, performance pales
            // in comparison with Groovy.
            this.compiledScript = compiledScript;
            this.scriptEngine = scriptEngine;
            this.bindings = setupBindings(scriptEngine);

            bindings.put("attributes", flowFile.getAttributes());
            bindings.put("log", getLogger());
        }

        @Override
        public Object evaluate(final Record record, final long index) throws ScriptException {
            bindings.put("record", record);
            bindings.put("recordIndex", index);

            compiledScript.eval(bindings);
            return scriptEngine.get("_");
        }
    }


    private class InterpretedScriptEvaluator implements ScriptEvaluator {
        private final ScriptEngine scriptEngine;
        private final String scriptToRun;
        private final Bindings bindings;

        public InterpretedScriptEvaluator(final ScriptEngine scriptEngine, final String scriptToRun, final FlowFile flowFile) {
            this.scriptEngine = scriptEngine;
            this.scriptToRun = scriptToRun;
            this.bindings = setupBindings(scriptEngine);

            bindings.put("attributes", flowFile.getAttributes());
            bindings.put("log", getLogger());
        }

        @Override
        public Object evaluate(final Record record, final long index) throws ScriptException {
            bindings.put("record", record);
            bindings.put("recordIndex", index);

            // Evaluate the script with the configurator (if it exists) or the engine
            return scriptEngine.eval(scriptToRun, bindings);
        }
    }

    private static class Counts {
        private long recordCount;
        private long droppedCount;

        public long getRecordCount() {
            return recordCount;
        }

        public long getDroppedCount() {
            return droppedCount;
        }

        public void incrementRecordCount() {
            recordCount++;
        }

        public void incrementDroppedCount() {
            droppedCount++;
        }
    }

    private interface RecordWriteAction {
        void write(Record record) throws IOException;
    }
}
