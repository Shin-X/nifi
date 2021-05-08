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

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.jolt.TransformFactory;
import org.apache.nifi.processors.standard.util.jolt.TransformUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr","cardinality","sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type",description = "Always set to application/json")
@CapabilityDescription("对flowfile JSON有效负载应用一个Jolt规格列表。使用转换后的内容创建一个新的流文件，并将其路由到“成功”关系。如果JSON转换失败，原始流文件将被路由到“failure”关系。")
public class JoltTransformJSON extends AbstractProcessor {

    public static final AllowableValue SHIFTR = new AllowableValue("jolt-transform-shift", "Shift", "Shift input JSON/data to create the output JSON.");
    public static final AllowableValue CHAINR = new AllowableValue("jolt-transform-chain", "Chain", "Execute list of Jolt transformations.");
    public static final AllowableValue DEFAULTR = new AllowableValue("jolt-transform-default", "Default", " Apply default values to the output JSON.");
    public static final AllowableValue REMOVR = new AllowableValue("jolt-transform-remove", "Remove", " Remove values from input data to create the output JSON.");
    public static final AllowableValue CARDINALITY = new AllowableValue("jolt-transform-card", "Cardinality", "Change the cardinality of input elements to create the output JSON.");
    public static final AllowableValue SORTR = new AllowableValue("jolt-transform-sort", "Sort", "Sort input json key values alphabetically. Any specification set is ignored.");
    public static final AllowableValue CUSTOMR = new AllowableValue("jolt-transform-custom", "Custom", "Custom Transformation. Requires Custom Transformation Class Name");
    public static final AllowableValue MODIFIER_DEFAULTR = new AllowableValue("jolt-transform-modify-default", "Modify - Default", "Writes when key is missing or value is null");
    public static final AllowableValue MODIFIER_OVERWRITER = new AllowableValue("jolt-transform-modify-overwrite", "Modify - Overwrite", " Always overwrite value");
    public static final AllowableValue MODIFIER_DEFINER = new AllowableValue("jolt-transform-modify-define", "Modify - Define", "Writes when key is missing");

    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jolt-transform")
            .displayName("震动转换DSL")
            .description("指定应与所提供的规范一起使用的震动转换。")
//            .displayName("Jolt Transformation DSL")
//            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(CARDINALITY, CHAINR, DEFAULTR, MODIFIER_DEFAULTR, MODIFIER_DEFINER, MODIFIER_OVERWRITER, REMOVR, SHIFTR, SORTR, CUSTOMR)
            .defaultValue(CHAINR.getValue())
            .build();

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("jolt-spec")
            .displayName("震动规范")
            .description("JSON数据转换的Jolt规范。如果选中了震动排序转换，则忽略此值。")
//            .displayName("Jolt Specification")
//            .description("Jolt Specification for transform of JSON data. This value is ignored if the Jolt Sort Transformation is selected.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor.Builder()
            .name("jolt-custom-class")
            .displayName("自定义转换类名")
            .description("用于自定义转换的全限定类名")
//            .displayName("Custom Transformation Class Name")
//            .description("Fully Qualified Class Name for Custom Transformation")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("jolt-custom-modules")
            .displayName("自定义模块目录")
            .description("以逗号分隔的文件和/或目录路径列表，这些路径包含包含自定义转换的模块(NiFi的类路径中不包含这些模块)。")
//            .displayName("Custom Module Directory")
//            .description("Comma-separated list of paths to files and/or directories which contain modules containing custom transformations (that are not included on NiFi's classpath).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("转换缓存大小")
            .description("编译一个Jolt转换可能相当昂贵。理想情况下，这只会执行一次。然而，如果在转换中使用表达式语言，我们可能需要为每个流文件创建一个新的转换。这个值控制我们在内存中缓存多少个转换，以避免每次都需要编译转换。")
//            .name("Transform Cache Size")
//            .description("Compiling a Jolt Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
//                + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("pretty_print")
            .displayName("优美打印")
            .description("应用优美打印格式到震动转换的输出")
//            .displayName("Pretty Print")
//            .description("Apply pretty print formatting to the output of the Jolt transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("成功")
            .description("带有转换后内容的流文件将被路由到这个关系")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("失败")
            .description("如果一个FlowFile由于任何原因无法处理(例如，这个FlowFile不是有效的JSON)，它将被路由到这个关系")
            .build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;
    private volatile ClassLoader customClassLoader;
    private final static String DEFAULT_CHARSET = "UTF-8";

    /**
     * It is a cache for transform objects. It keep values indexed by jolt specification string.
     * For some cases the key could be empty. It means that it represents default transform (e.g. for custom transform
     * when there is no jolt-record-spec specified).
     */
    private LoadingCache<Optional<String>, JoltTransform> transformCache;

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(JOLT_TRANSFORM);
        _properties.add(CUSTOM_CLASS);
        _properties.add(MODULES);
        _properties.add(JOLT_SPEC);
        _properties.add(TRANSFORM_CACHE_SIZE);
        _properties.add(PRETTY_PRINT);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String transform = validationContext.getProperty(JOLT_TRANSFORM).getValue();
        final String customTransform = validationContext.getProperty(CUSTOM_CLASS).getValue();
        final String modulePath = validationContext.getProperty(MODULES).isSet()? validationContext.getProperty(MODULES).getValue() : null;

        if(!validationContext.getProperty(JOLT_SPEC).isSet() || StringUtils.isEmpty(validationContext.getProperty(JOLT_SPEC).getValue())){
            if(!SORTR.getValue().equals(transform)) {
                final String message = "A specification is required for this transformation";
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        } else {
            final ClassLoader customClassLoader;

            try {
                if (modulePath != null) {
                    customClassLoader = ClassLoaderUtils.getCustomClassLoader(modulePath, this.getClass().getClassLoader(), getJarFilenameFilter());
                } else {
                    customClassLoader =  this.getClass().getClassLoader();
                }

                final String specValue =  validationContext.getProperty(JOLT_SPEC).getValue();

                if (validationContext.isExpressionLanguagePresent(specValue)) {
                    final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(specValue,true);
                    if (!StringUtils.isEmpty(invalidExpressionMsg)) {
                        results.add(new ValidationResult.Builder().valid(false)
                                .subject(JOLT_SPEC.getDisplayName())
                                .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                                .build());
                    }
                } else {
                    //for validation we want to be able to ensure the spec is syntactically correct and not try to resolve variables since they may not exist yet
                    Object specJson = SORTR.getValue().equals(transform) ? null : JsonUtils.jsonToObject(specValue.replaceAll("\\$\\{","\\\\\\\\\\$\\{"), DEFAULT_CHARSET);

                    if (CUSTOMR.getValue().equals(transform)) {
                        if (StringUtils.isEmpty(customTransform)) {
                            final String customMessage = "A custom transformation class should be provided. ";
                            results.add(new ValidationResult.Builder().valid(false)
                                    .explanation(customMessage)
                                    .build());
                        } else {
                            TransformFactory.getCustomTransform(customClassLoader, customTransform, specJson);
                        }
                    } else {
                        TransformFactory.getTransform(customClassLoader, transform, specJson);
                    }
                }
            } catch (final Exception e) {
                getLogger().info("Processor is not valid - " + e.toString());
                String message = "Specification not valid for the selected transformation." ;
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final Object inputJson;
        try (final InputStream in = session.read(original)) {
            inputJson = JsonUtils.jsonToObject(in);
        } catch (final Exception e) {
            logger.error("Failed to transform {}; routing to failure", new Object[] {original, e});
            session.transfer(original, REL_FAILURE);
            return;
        }

        final String jsonString;
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final JoltTransform transform = getTransform(context, original);
            if (customClassLoader != null) {
                Thread.currentThread().setContextClassLoader(customClassLoader);
            }

            final Object transformedJson = TransformUtils.transform(transform,inputJson);
            jsonString = context.getProperty(PRETTY_PRINT).asBoolean() ? JsonUtils.toPrettyJsonString(transformedJson) : JsonUtils.toJsonString(transformedJson);
        } catch (final Exception ex) {
            logger.error("Unable to transform {} due to {}", new Object[] {original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        } finally {
            if (customClassLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }

        FlowFile transformed = session.write(original, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString.getBytes(DEFAULT_CHARSET));
            }
        });

        final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
        transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"Modified With " + transformType ,stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        logger.info("Transformed {}", new Object[]{original});
    }

    private JoltTransform getTransform(final ProcessContext context, final FlowFile flowFile) throws Exception {
        final Optional<String> specString;
        if (context.getProperty(JOLT_SPEC).isSet()) {
            specString = Optional.of(context.getProperty(JOLT_SPEC).evaluateAttributeExpressions(flowFile).getValue());
        } else {
            specString = Optional.empty();
        }

        return transformCache.get(specString);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build(specString -> createTransform(context, specString.orElse(null)));

        try {
            if (context.getProperty(MODULES).isSet()) {
                customClassLoader = ClassLoaderUtils.getCustomClassLoader(context.getProperty(MODULES).getValue(), this.getClass().getClassLoader(), getJarFilenameFilter());
            } else {
                customClassLoader = this.getClass().getClassLoader();
            }
        } catch (final Exception ex) {
            getLogger().error("Unable to setup processor", ex);
        }
    }

    private JoltTransform createTransform(final ProcessContext context, final String specString) throws Exception {
        final Object specJson;
        if (context.getProperty(JOLT_SPEC).isSet() && !SORTR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            specJson = JsonUtils.jsonToObject(specString, DEFAULT_CHARSET);
        } else {
            specJson = null;
        }

        if (CUSTOMR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            return TransformFactory.getCustomTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(CUSTOM_CLASS).getValue(), specJson);
        } else {
            return TransformFactory.getTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
        }
    }

    protected FilenameFilter getJarFilenameFilter(){
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

}
