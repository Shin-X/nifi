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
package org.apache.nifi.processors.standard;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;
import org.apache.tika.mime.MimeTypeException;


/**
 * <p>
 * Attempts to detect the MIME Type of a FlowFile by examining its contents. If the MIME Type is determined, it is added
 * to an attribute with the name mime.type. In addition, mime.extension is set if a common file extension is known.
 * </p>
 *
 * <p>
 * MIME Type detection is performed by Apache Tika; more information about detection is available at http://tika.apache.org.
 *
 * <ul>
 * <li>application/flowfile-v3</li>
 * <li>application/flowfile-v1</li>
 * </ul>
 * </p>
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"compression", "gzip", "bzip2", "zip", "MIME", "mime.type", "file", "identify"})
@CapabilityDescription("尝试识别用于流文件的MIME类型。如果MIME类型可以被识别，则使用名为' MIME '的属性。type'的值被添加为MIME类型。如果无法确定MIME类型，则该值将被设置为'application/octet-stream'。此外，属性mime。如果已知MIME类型的公共文件扩展名，则设置扩展名。如果配置文件和配置主体都没有设置，则使用默认的NiFi MIME类型。")
@WritesAttributes({
@WritesAttribute(attribute = "mime.type", description = "This Processor sets the FlowFile's mime.type attribute to the detected MIME Type. "
        + "If unable to detect the MIME Type, the attribute's value will be set to application/octet-stream"),
@WritesAttribute(attribute = "mime.extension", description = "This Processor sets the FlowFile's mime.extension attribute to the file "
        + "extension associated with the detected MIME Type. "
        + "If there is no correlated extension, the attribute's value will be empty")
}
)
public class IdentifyMimeType extends AbstractProcessor {

    public static final PropertyDescriptor USE_FILENAME_IN_DETECTION = new PropertyDescriptor.Builder()
           .displayName("Use Filename In Detection")
           .name("use-filename-in-detection")
           .description("If true will pass the filename to Tika to aid in detection.")
           .required(true)
           .allowableValues("true", "false")
           .defaultValue("true")
           .build();

    public static final PropertyDescriptor MIME_CONFIG_FILE = new PropertyDescriptor.Builder()
            .displayName("Config File")
            .name("config-file")
            .required(false)
            .description("Path to MIME type config file. Only one of Config File or Config Body may be used.")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIME_CONFIG_BODY = new PropertyDescriptor.Builder()
            .displayName("Config Body")
            .name("config-body")
            .required(false)
            .description("Body of MIME type config file. Only one of Config File or Config Body may be used.")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final TikaConfig config;
    private Detector detector;
    private MimeTypes mimeTypes;

    public IdentifyMimeType() {
        this.config = TikaConfig.getDefaultConfig();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(USE_FILENAME_IN_DETECTION);
        properties.add(MIME_CONFIG_BODY);
        properties.add(MIME_CONFIG_FILE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        String configBody = context.getProperty(MIME_CONFIG_BODY).getValue();
        String configFile = context.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().getValue();

        if (configBody == null && configFile == null){
            this.detector = config.getDetector();
            this.mimeTypes = config.getMimeRepository();
        } else if (configBody != null) {
            try {
                this.detector = MimeTypesFactory.create(new ByteArrayInputStream(configBody.getBytes()));
                this.mimeTypes = (MimeTypes)this.detector;
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load config body", e);
            }

        } else {
            try (final FileInputStream fis = new FileInputStream(configFile);
                 final InputStream bis = new BufferedInputStream(fis)) {
                this.detector = MimeTypesFactory.create(bis);
                this.mimeTypes = (MimeTypes)this.detector;
            } catch (Exception e) {
                context.yield();
                throw new ProcessException("Failed to load config file", e);
            }
        }
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream stream) throws IOException {
                try (final InputStream in = new BufferedInputStream(stream);
                     final TikaInputStream tikaStream = TikaInputStream.get(in)) {
                    Metadata metadata = new Metadata();

                    if (filename != null && context.getProperty(USE_FILENAME_IN_DETECTION).asBoolean()) {
                        metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, filename);
                    }
                    // Get mime type
                    MediaType mediatype = detector.detect(tikaStream, metadata);
                    mimeTypeRef.set(mediatype.toString());
                }
            }
        });

        String mimeType = mimeTypeRef.get();
        String extension = "";
        try {
            MimeType mimetype;
            mimetype = mimeTypes.forName(mimeType);
            extension = mimetype.getExtension();
        } catch (MimeTypeException ex) {
            logger.warn("MIME type extension lookup failed: {}", new Object[]{ex});
        }

        // Workaround for bug in Tika - https://issues.apache.org/jira/browse/TIKA-1563
        if (mimeType != null && mimeType.equals("application/gzip") && extension.equals(".tgz")) {
            extension = ".gz";
        }

        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            flowFile = session.putAttribute(flowFile, "mime.extension", "");
            logger.info("Unable to identify MIME Type for {}; setting to application/octet-stream", new Object[]{flowFile});
        } else {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
            flowFile = session.putAttribute(flowFile, "mime.extension", extension);
            logger.info("Identified {} as having MIME Type {}", new Object[]{flowFile, mimeType});
        }

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String body = validationContext.getProperty(MIME_CONFIG_BODY).getValue();
        String file = validationContext.getProperty(MIME_CONFIG_FILE).getValue();
        if(body != null && file != null) {
            results.add(new ValidationResult.Builder()
                    .input(MIME_CONFIG_FILE.getName())
                    .subject(file)
                    .valid(false)
                    .explanation("Can only specify Config Body or Config File.  Not both.")
                    .build());
        }
        return results;
    }

}
