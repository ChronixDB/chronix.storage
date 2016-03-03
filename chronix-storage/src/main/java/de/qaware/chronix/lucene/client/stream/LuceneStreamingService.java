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
package de.qaware.chronix.lucene.client.stream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import de.qaware.chronix.Schema;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.lucene.client.stream.date.DateQueryParser;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.Executors;

/**
 * The lucene streaming service let one stream data from a lucene index.
 *
 * @param <T> type of the returned class
 * @author f.lautenschlager
 */
public class LuceneStreamingService<T> implements Iterator<T> {

    /**
     * The class logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneStreamingService.class);

    /**
     * The query and connection to solr
     */
    private final Query query;
    private final IndexSearcher searcher;

    /**
     * Converter for converting the documents
     */
    private final TimeSeriesConverter<T> converter;

    /**
     * Query parameters
     */
    private int nrOfTimeSeriesPerBatch;
    private long nrOfAvailableTimeSeries = -1;
    private int currentDocumentCount = 0;


    /**
     * The executor service to do the work asynchronously
     */
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());


    /**
     * Start and end of the query to filter points on client side
     */
    private long queryStart;
    private long queryEnd;

    private TimeSeriesHandler<T> timeSeriesHandler;

    /**
     * Constructs a streaming service
     *
     * @param converter              - the converter to convert documents
     * @param query                  - the lucene query
     * @param searcher               - the index search
     * @param nrOfTimeSeriesPerBatch - the number of time series that are read by one query
     */
    public LuceneStreamingService(TimeSeriesConverter<T> converter, Query query, IndexSearcher searcher, int nrOfTimeSeriesPerBatch) {
        this.converter = converter;
        this.query = query;
        this.searcher = searcher;
        this.nrOfTimeSeriesPerBatch = nrOfTimeSeriesPerBatch;
        this.timeSeriesHandler = new TimeSeriesHandler<>(200);
        parseDates(query);
    }

    private void parseDates(Query query) {
        DateQueryParser dateRangeParser = new DateQueryParser(new String[]{Schema.START, Schema.END});
        long[] startAndEnd = new long[0];
        try {
            startAndEnd = dateRangeParser.getNumericQueryTerms(query.toString());

        } catch (ParseException e) {
            LOGGER.warn("Could not parse start or end", e);
        }
        this.queryStart = or(startAndEnd[0], -1, 0);
        this.queryEnd = or(startAndEnd[1], -1, Long.MAX_VALUE);
    }

    private long or(long value, long condition, long or) {
        if (value == condition) {
            return or;
        } else {
            return value;
        }
    }

    @Override
    public boolean hasNext() {
        if (nrOfAvailableTimeSeries == -1) {
            try {
                nrOfAvailableTimeSeries = searcher.count(query);
            } catch (IOException e) {
                LOGGER.error("Could not count the found documents", e);
            }
        }

        return currentDocumentCount < nrOfAvailableTimeSeries;
    }

    @Override
    public T next() {
        if (currentDocumentCount % nrOfTimeSeriesPerBatch == 0) {
            try {
                ScoreDoc[] hits = searcher.search(query, nrOfTimeSeriesPerBatch).scoreDocs;
                convertHits(hits);
            } catch (IOException e) {
                LOGGER.info("Could not search documents");
            }
        }
        currentDocumentCount++;
        return timeSeriesHandler.take();
    }


    private void convertHits(ScoreDoc[] hits) throws IOException {
        for (ScoreDoc hit : hits) {
            Document hitDoc = searcher.doc(hit.doc);
            ListenableFuture future = service.submit(new TimeSeriesConverterCaller<>(hitDoc, converter, queryStart, queryEnd));
            Futures.addCallback(future, timeSeriesHandler);
        }
    }

}
