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

package org.apache.nifi.processors.elasticsearch

import groovy.json.JsonSlurper
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.elasticsearch.DeleteOperationResponse
import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.elasticsearch.SearchResponse

class TestElasticsearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    private boolean returnAggs
    private boolean throwErrorInSearch
    private boolean throwErrorInDelete

    TestElasticsearchClientService(boolean returnAggs) {
        this.returnAggs = returnAggs
    }

    @Override
    IndexOperationResponse add(IndexOperationRequest operation) {
        return add(Arrays.asList(operation))
    }

    @Override
    IndexOperationResponse bulk(List<IndexOperationRequest> operations) {
        return new IndexOperationResponse(100L, 100L)
    }

    @Override
    Long count(String query, String index, String type) {
        return null
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, String id) {
        return deleteById(index, type, Arrays.asList(id))
    }

    @Override
    DeleteOperationResponse deleteById(String index, String type, List<String> ids) {
        if (throwErrorInDelete) {
            throw new IOException("Simulated IOException")
        }
        return new DeleteOperationResponse(100L)
    }

    @Override
    DeleteOperationResponse deleteByQuery(String query, String index, String type) {
        return deleteById(index, type, Arrays.asList("1"))
    }

    @Override
    Map<String, Object> get(String index, String type, String id) {
        return [ "msg": "one" ]
    }

    @Override
    SearchResponse search(String query, String index, String type) {
        if (throwErrorInSearch) {
            throw new IOException("Simulated IOException")
        }

        def mapper = new JsonSlurper()
        def hits = mapper.parseText(HITS_RESULT)
        def aggs = returnAggs ?  mapper.parseText(AGGS_RESULT) :  null
        SearchResponse response = new SearchResponse(hits, aggs, 15, 5, false)
        return response
    }

    @Override
    String getTransitUrl(String index, String type) {
        "http://localhost:9400/${index}/${type}"
    }

    private static final String AGGS_RESULT = "{\n" +
            "    \"term_agg2\": {\n" +
            "      \"doc_count_error_upper_bound\": 0,\n" +
            "      \"sum_other_doc_count\": 0,\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"key\": \"five\",\n" +
            "          \"doc_count\": 5\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"four\",\n" +
            "          \"doc_count\": 4\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"three\",\n" +
            "          \"doc_count\": 3\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"two\",\n" +
            "          \"doc_count\": 2\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"one\",\n" +
            "          \"doc_count\": 1\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"term_agg\": {\n" +
            "      \"doc_count_error_upper_bound\": 0,\n" +
            "      \"sum_other_doc_count\": 0,\n" +
            "      \"buckets\": [\n" +
            "        {\n" +
            "          \"key\": \"five\",\n" +
            "          \"doc_count\": 5\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"four\",\n" +
            "          \"doc_count\": 4\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"three\",\n" +
            "          \"doc_count\": 3\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"two\",\n" +
            "          \"doc_count\": 2\n" +
            "        },\n" +
            "        {\n" +
            "          \"key\": \"one\",\n" +
            "          \"doc_count\": 1\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }"

    private static final String HITS_RESULT = "[\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"14\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"5\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"8\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"9\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"10\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"four\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"12\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"2\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"two\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"4\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"6\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"three\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"_index\": \"messages\",\n" +
            "        \"_type\": \"message\",\n" +
            "        \"_id\": \"15\",\n" +
            "        \"_score\": 1,\n" +
            "        \"_source\": {\n" +
            "          \"msg\": \"five\"\n" +
            "        }\n" +
            "      }\n" +
            "    ]"

    void setThrowErrorInSearch(boolean throwErrorInSearch) {
        this.throwErrorInSearch = throwErrorInSearch
    }

    void setThrowErrorInDelete(boolean throwErrorInDelete) {
        this.throwErrorInDelete = throwErrorInDelete
    }
}
