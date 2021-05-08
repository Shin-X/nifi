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
package org.apache.nifi.provenance.lucene;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardQueryResult;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexSearch {
    private final Logger logger = LoggerFactory.getLogger(IndexSearch.class);
    private final PersistentProvenanceRepository repository;
    private final File indexDirectory;
    private final IndexManager indexManager;
    private final int maxAttributeChars;

    public IndexSearch(final PersistentProvenanceRepository repo, final File indexDirectory, final IndexManager indexManager, final int maxAttributeChars) {
        this.repository = repo;
        this.indexDirectory = indexDirectory;
        this.indexManager = indexManager;
        this.maxAttributeChars = maxAttributeChars;
    }

    public StandardQueryResult search(final org.apache.nifi.provenance.search.Query provenanceQuery, final NiFiUser user, final AtomicInteger retrievedCount,
        final long firstEventTimestamp) throws IOException {
        if (retrievedCount.get() >= provenanceQuery.getMaxResults()) {
            final StandardQueryResult sqr = new StandardQueryResult(provenanceQuery, 1);
            sqr.update(Collections.<ProvenanceEventRecord> emptyList(), 0L);

            logger.info("Skipping search of Provenance Index {} for {} because the max number of results ({}) has already been retrieved",
                indexDirectory, provenanceQuery, provenanceQuery.getMaxResults());

            return sqr;
        }

        final long startNanos = System.nanoTime();

        if (!indexDirectory.exists() && !indexDirectory.mkdirs()) {
            throw new IOException("Unable to create Indexing Directory " + indexDirectory);
        }
        if (!indexDirectory.isDirectory()) {
            throw new IOException("Indexing Directory specified is " + indexDirectory + ", but this is not a directory");
        }

        final StandardQueryResult sqr = new StandardQueryResult(provenanceQuery, 1);
        final Set<ProvenanceEventRecord> matchingRecords;

        final Query luceneQuery = LuceneUtil.convertQuery(provenanceQuery);

        final long start = System.nanoTime();
        EventIndexSearcher searcher = null;
        try {
            searcher = indexManager.borrowIndexSearcher(indexDirectory);
            final long searchStartNanos = System.nanoTime();
            final long openSearcherNanos = searchStartNanos - start;

            logger.debug("Searching {} for {}", this, provenanceQuery);
            final TopDocs topDocs = searcher.getIndexSearcher().search(luceneQuery, provenanceQuery.getMaxResults());
            final long finishSearch = System.nanoTime();
            final long searchNanos = finishSearch - searchStartNanos;

            logger.debug("Searching {} for {} took {} millis; opening searcher took {} millis", this, provenanceQuery,
                    TimeUnit.NANOSECONDS.toMillis(searchNanos), TimeUnit.NANOSECONDS.toMillis(openSearcherNanos));

            if (topDocs.totalHits.value == 0) {
                sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
                return sqr;
            }

            final DocsReader docsReader = new DocsReader();

            final EventAuthorizer authorizer = new EventAuthorizer() {
                @Override
                public boolean isAuthorized(ProvenanceEventRecord event) {
                    return repository.isAuthorized(event, user);
                }

                @Override
                public void authorize(ProvenanceEventRecord event) throws AccessDeniedException {
                    repository.authorize(event, user);
                }

                @Override
                public List<ProvenanceEventRecord> filterUnauthorizedEvents(List<ProvenanceEventRecord> events) {
                    return repository.filterUnauthorizedEvents(events, user);
                }

                @Override
                public Set<ProvenanceEventRecord> replaceUnauthorizedWithPlaceholders(Set<ProvenanceEventRecord> events) {
                    return repository.replaceUnauthorizedWithPlaceholders(events, user);
                }
            };

            matchingRecords = docsReader.read(topDocs, authorizer, searcher.getIndexSearcher().getIndexReader(), repository.getAllLogFiles(), retrievedCount,
                provenanceQuery.getMaxResults(), maxAttributeChars);

            final long readRecordsNanos = System.nanoTime() - finishSearch;
            logger.debug("Reading {} records took {} millis for {}", matchingRecords.size(), TimeUnit.NANOSECONDS.toMillis(readRecordsNanos), this);

            sqr.update(matchingRecords, topDocs.totalHits.value);

            final long queryNanos = System.nanoTime() - startNanos;
            logger.info("Successfully executed {} against Index {}; Search took {} milliseconds; Total Hits = {}",
                provenanceQuery, indexDirectory, TimeUnit.NANOSECONDS.toMillis(queryNanos), topDocs.totalHits);

            return sqr;
        } catch (final FileNotFoundException e) {
            // nothing has been indexed yet, or the data has already aged off
            logger.warn("Attempted to search Provenance Index {} but could not find the file due to {}", indexDirectory, e);
            if ( logger.isDebugEnabled() ) {
                logger.warn("", e);
            }

            sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
            return sqr;
        } finally {
            if ( searcher != null ) {
                indexManager.returnIndexSearcher(searcher);
            }
        }
    }


    @Override
    public String toString() {
        return "IndexSearcher[" + indexDirectory + "]";
    }
}
