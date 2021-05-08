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
package org.apache.nifi;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Tags({"put", "html", "dom", "css", "element"})
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("在现有的HTML DOM中放置一个新的HTML元素。新HTML元素所需的位置是通过使用CSS选择器语法指定的。传入的HTML首先被转换成HTML文档对象模型，这样HTML DOM的位置就可以以类似于CSS选择器用于将样式应用到HTML的方式定位。然后，使用用户定义的CSS选择器字符串来查找用户希望添加新HTML元素的位置，从而生成HTML DOM。一旦新的HTML元素添加到DOM中，它就会呈现为HTML，结果将用更新后的HTML替换flowfile内容。更详细的CSS选择器语法参考可以在\"http://jsoup.org/apidocs/org/jsoup/select/Selector.html\"找到")
@SeeAlso({GetHTMLElement.class, ModifyHTMLElement.class})
public class PutHTMLElement extends AbstractHTMLProcessor {

    public static final String APPEND_ELEMENT = "append-html";
    public static final String PREPEND_ELEMENT = "prepend-html";

    public static final PropertyDescriptor PUT_LOCATION_TYPE = new PropertyDescriptor.Builder()
            .name("Element Insert Location Type")
            .description("Controls whether the new element is prepended or appended to the children of the " +
                    "Element located by the CSS selector. EX: prepended value '<b>Hi</b>' inside of " +
                    "Element (using CSS Selector 'p') '<p>There</p>' would result in " +
                    "'<p><b>Hi</b>There</p>'. Appending the value would result in '<p>There<b>Hi</b></p>'")
            .required(true)
            .allowableValues(APPEND_ELEMENT, PREPEND_ELEMENT)
            .defaultValue(APPEND_ELEMENT)
            .build();

    public static final PropertyDescriptor PUT_VALUE = new PropertyDescriptor.Builder()
            .name("Put Value")
            .description("Value used when creating the new Element. Value should be a valid HTML element. " +
                    "The text should be supplied unencoded: characters like '<', '>', etc will be properly HTML " +
                    "encoded in the resulting output.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CSS_SELECTOR);
        descriptors.add(HTML_CHARSET);
        descriptors.add(PUT_LOCATION_TYPE);
        descriptors.add(PUT_VALUE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_INVALID_HTML);
        relationships.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * This processor used to support URL property, but it has been removed
     * since it's not required when altering HTML elements.
     * Support URL as dynamic property so that existing data flow can stay in valid state without modification.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return URL;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Document doc;
        final Elements eles;
        try {
            doc = parseHTMLDocumentFromFlowfile(flowFile, context, session);
            eles = doc.select(context.getProperty(CSS_SELECTOR).evaluateAttributeExpressions(flowFile).getValue());
        } catch (Exception ex) {
            getLogger().error("Failed to extract HTML from {} due to {}; routing to {}", new Object[] {flowFile, ex.toString(), REL_INVALID_HTML.getName()}, ex);
            session.transfer(flowFile, REL_INVALID_HTML);
            return;
        }


        if (eles == null || eles.isEmpty()) {
            // No element found
            session.transfer(flowFile, REL_NOT_FOUND);
        } else {
            final String putValue = context.getProperty(PUT_VALUE).evaluateAttributeExpressions(flowFile).getValue();

            for (final Element ele : eles) {
                switch (context.getProperty(PUT_LOCATION_TYPE).getValue()) {
                    case APPEND_ELEMENT:
                        ele.append(putValue);
                        break;
                    case PREPEND_ELEMENT:
                        ele.prepend(putValue);
                        break;
                }
            }

            FlowFile ff = session.write(session.create(flowFile), new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    out.write(doc.html().getBytes(StandardCharsets.UTF_8));
                }
            });

            session.transfer(ff, REL_SUCCESS);

            // Transfer the original HTML
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }
}
