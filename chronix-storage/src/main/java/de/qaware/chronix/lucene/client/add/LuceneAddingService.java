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
package de.qaware.chronix.lucene.client.add;

import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.lucene.client.ValueConverterHelper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * A service class to add time series to Apache Solr.
 **/
public final class LuceneAddingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneAddingService.class);

    private LuceneAddingService() {
        //Avoid instances
    }

    /**
     * Adds the given collection of time series to Apache Solr.
     * Note: The add method do not commit the time series
     *
     * @param converter   - the converter to converter the time series into a Solr document
     * @param timeSeries  - the collection with time series
     * @param indexWriter - the connection to the Apache solr.
     * @return true if successful, otherwise false
     */
    public static <T> boolean add(TimeSeriesConverter<T> converter, Collection<T> timeSeries, IndexWriter indexWriter) {

        if (timeSeries == null || timeSeries.isEmpty()) {
            LOGGER.debug("Collection is empty. Nothing to commit");
            return true;
        }

        timeSeries.parallelStream().forEach(ts -> {
            try {
                Document luceneDoc = convert(ts, converter);
                indexWriter.addDocument(luceneDoc);
            } catch (IOException e) {
                LOGGER.error("Could not add documents to lucene.", e);
            }
        });
        return true;
    }

    /**
     * Converts a time series of type <T> to SolInputDocument
     *
     * @param ts - the time series
     * @return a filled SolrInputDocument
     */
    private static <T> Document convert(T ts, TimeSeriesConverter<T> converter) {
        BinaryTimeSeries series = converter.to(ts);
        Document document = new Document();

        series.getFields().entrySet().forEach(entry -> {

            if (entry.getValue() instanceof Number) {
                ValueConverterHelper.handleNumbers(document, entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof String || entry.getValue() instanceof byte[]) {
                ValueConverterHelper.handleStringsAndBytes(document, entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof Collection || entry.getValue() instanceof Object[]) {
                ValueConverterHelper.handleCollectionsAndArrays(document, entry);
            } else {
                LOGGER.debug("Field {} could not be handled. Type is not supported", entry);
            }
        });
        return document;
    }

}
