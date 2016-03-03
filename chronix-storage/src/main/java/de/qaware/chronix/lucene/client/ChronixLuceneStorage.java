/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.lucene.client;

import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.lucene.client.add.LuceneAddingService;
import de.qaware.chronix.lucene.client.stream.LuceneStreamingService;
import de.qaware.chronix.streaming.StorageService;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

/**
 * Lucene storage implementation of the Chronix StorageService interface
 *
 * @param <T> - the time series type
 */
public final class ChronixLuceneStorage<T> implements StorageService<T, LuceneIndex, Query> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronixLuceneStorage.class);

    private final int nrOfDocumentPerBatch;
    private final BinaryOperator<T> reduce;
    private final Function<T, String> groupBy;

    /**
     * Constructs a Chronix storage that is based on Apache solr.
     *
     * @param nrOfDocumentPerBatch number of documents that are processed in one batch
     * @param groupBy              the function to group time series records
     * @param reduce               the function to reduce the grouped time series records into one time series
     */
    public ChronixLuceneStorage(final int nrOfDocumentPerBatch, final Function<T, String> groupBy, final BinaryOperator<T> reduce) {
        this.nrOfDocumentPerBatch = nrOfDocumentPerBatch;
        this.groupBy = groupBy;
        this.reduce = reduce;
    }

    /**
     * Queries apache solr and returns the time series in a stream.
     *
     * @param converter the time series converter
     * @param index     the connection to apache solr
     * @param query     the user query
     * @return a stream of time series
     */
    @Override
    public Stream<T> stream(TimeSeriesConverter<T> converter, LuceneIndex index, Query query) {
        LOGGER.debug("Streaming data from lucene using converter {}, Lucene Index {}, and Lucene Query {}", converter, index, query);
        try {
            LuceneStreamingService<T> luceneStreamingService = new LuceneStreamingService<>(converter, query, index.getSearcher(), nrOfDocumentPerBatch);

            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(luceneStreamingService, Spliterator.SIZED), false)
                    .filter(t -> t != null)//Remove empty results
                    .collect(groupingBy((Function<T, String>) groupBy::apply)).values().stream()
                    .map(ts -> ts.stream().reduce(reduce).get());

        } catch (IOException e) {
            LOGGER.error("Could not open the lucene index searcher", e);
        }
        return Stream.empty();
    }

    /**
     * Adds the given collection of documents to the solr connection using the collector.
     * Note: The function does not call commit on the connection. Documents are just added to lucene.
     *
     * @param converter   the converter matching the type <T>
     * @param documents   the documents of type <T>
     * @param luceneIndex the lucene index
     * @return true if the documents are added to apache solr.
     */
    @Override
    public boolean add(TimeSeriesConverter<T> converter, Collection<T> documents, LuceneIndex luceneIndex) {
        try {
            return LuceneAddingService.add(converter, documents, luceneIndex.getOpenWriter());
        } catch (IOException e) {
            LOGGER.error("Could not open lucene index writer", e);
        }
        return false;
    }


}
