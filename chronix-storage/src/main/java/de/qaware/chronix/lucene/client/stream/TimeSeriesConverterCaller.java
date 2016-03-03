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


import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.lucene.client.ValueConverterHelper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Converts the lucene document into a binary time series and calls the given document converter.
 *
 * @param <T> the type of the returned time series class
 * @author f.lautenschlager
 */
public class TimeSeriesConverterCaller<T> implements Callable<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesConverterCaller.class);

    private final Document document;
    private final TimeSeriesConverter<T> documentConverter;
    private final long queryEnd;
    private final long queryStart;

    /**
     * Constructs a SolrDocumentConverter.
     *
     * @param document          - the fields and values
     * @param documentConverter - the concrete document converter
     */
    public TimeSeriesConverterCaller(final Document document, final TimeSeriesConverter<T> documentConverter, long queryStart, long queryEnd) {
        this.document = document;
        this.documentConverter = documentConverter;
        this.queryStart = queryStart;
        this.queryEnd = queryEnd;
    }

    /**
     * Converts the solr document given in the constructor into a time series of type <T>
     *
     * @return a time series of type <T>
     * @throws Exception if bad things happen.
     */
    @Override
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public T call() throws Exception {
        BinaryTimeSeries.Builder timeSeriesBuilder = new BinaryTimeSeries.Builder();

        Map<String, List<Object>> multivalued = new HashMap<>();

        document.forEach(attributeField -> {
            String key = attributeField.name();

            if (key.contains(ValueConverterHelper.MULTI_VALUE_FIELD_DELIMITER)) {
                key = key.substring(0, key.indexOf(ValueConverterHelper.MULTI_VALUE_FIELD_DELIMITER));
                //Handle multivalued fields
                if (!multivalued.containsKey(key)) {
                    multivalued.put(key, new ArrayList<>());
                }
                multivalued.get(key).add(convert(attributeField));

            } else {
                timeSeriesBuilder.field(key, convert(attributeField));
            }
        });
        multivalued.forEach(timeSeriesBuilder::field);

        LOGGER.debug("Calling document converter with {}", document);
        T timeSeries = documentConverter.from(timeSeriesBuilder.build(), queryStart, queryEnd);
        LOGGER.debug("Returning time series {} to callee", timeSeries);
        return timeSeries;
    }

    /**
     * Adds user defined attributes to the binary time series builder.
     * Checks if the attribute is of type byte[], String, Number or Collection.
     * Otherwise the attribute is ignored.
     *
     * @param field the attribute field
     */
    private Object convert(IndexableField field) {
        LOGGER.debug("Reading field {} ", field);

        if (field.numericValue() != null) {
            return field.numericValue();
        } else if (field.stringValue() != null) {
            return field.stringValue();
        } else if (field.binaryValue() != null) {
            return field.binaryValue().bytes;
        } else {
            LOGGER.debug("Field {} could not be handled. Type is not supported", field);
            return null;
        }
    }
}

