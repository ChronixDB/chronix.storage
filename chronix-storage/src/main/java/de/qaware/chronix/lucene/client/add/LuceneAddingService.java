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
import de.qaware.chronix.lucene.client.ChronixLuceneStorageConstants;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A service class to add time series to lucene.
 **/
public final class LuceneAddingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneAddingService.class);

    private LuceneAddingService() {
        //Avoid instances
    }

    /**
     * Adds the given collection of time series to the lucene index.
     * Converts the time series using the default object types of java and available lucene fields.
     * If an attribute of a time series is user defined data type then it is ignored.
     * <p>
     * Note: The add method do not commit the time series.
     *
     * @param converter   the converter to converter the time series into a lucene document
     * @param timeSeries  the collection with time series
     * @param indexWriter the lucene index writer
     * @return true if successful, otherwise false
     */
    public static <T> boolean add(TimeSeriesConverter<T> converter, Collection<T> timeSeries, IndexWriter indexWriter) {

        if (timeSeries == null || timeSeries.isEmpty()) {
            LOGGER.debug("Collection is empty. Nothing to commit");
            return true;
        }

        timeSeries.parallelStream().forEach(ts -> {
            try {
                indexWriter.addDocument(convert(ts, converter));
            } catch (IOException e) {
                LOGGER.error("Could not add documents to lucene.", e);
            }
        });
        return true;
    }

    /**
     * Converts a time series of type <T> to lucene document.
     * Handles the default java object types (e.g. double, int, array, collections, ...)
     * and wraps them into the matching lucene fields (int -> IntField).
     *
     * @param ts the time series of type <T>
     * @return a filled lucene document
     */
    private static <T> Document convert(T ts, TimeSeriesConverter<T> converter) {
        BinaryTimeSeries series = converter.to(ts);
        Document document = new Document();

        series.getFields().entrySet().forEach(entry -> {

            if (entry.getValue() instanceof Number) {
                handleNumbers(document, entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof String || entry.getValue() instanceof byte[]) {
                handleStringsAndBytes(document, entry.getKey(), entry.getValue());
            } else if (entry.getValue() instanceof Collection || entry.getValue() instanceof Object[]) {
                handleArraysAndIterable(document, entry.getKey(), entry.getValue());
            } else {
                LOGGER.debug("Field {} could not be handled. Type is not supported", entry);
            }
        });
        return document;
    }

    /**
     * Tries to cast field value (object) to an array or iterable.
     * If the field value is not an array or iterable then the method ignores the field.
     * <p>
     * If the value is an array or iterable than the value is warped into a matching lucene field (Field for String,
     * StoredField for byte[]) and added to the lucene document.
     *
     * @param document   the lucene document to add the number
     * @param fieldName  the field name
     * @param fieldValue the field value
     */
    private static void handleArraysAndIterable(Document document, String fieldName, Object fieldValue) {

        //If have an array, simple convert it into an list.
        if (fieldValue != null && fieldValue.getClass().isArray()) {
            fieldValue = Arrays.asList((Object[]) fieldValue);
        }
        //Handle all iterable data types
        if (fieldValue instanceof Iterable) {
            Collection objects = (Collection) fieldValue;

            int fieldCounter = 0;
            fieldName = fieldName + ChronixLuceneStorageConstants.MULTI_VALUE_FIELD_DELIMITER;
            for (Object o : objects) {
                fieldCounter++;
                handleNumbers(document, fieldName + fieldCounter, o);
                handleStringsAndBytes(document, fieldName + fieldCounter, o);
            }
        }
    }

    /**
     * Tries to cast field value (object) to a string or byte[].
     * If the field value is not a string or a byte[] then the method ignores the field.
     * <p>
     * If the value is a string or byte[] than the value is warped into a matching lucene field (Field for String,
     * StoredField for byte[]) and added to the lucene document.
     *
     * @param document   the lucene document to add the number
     * @param fieldName  the field name
     * @param fieldValue the field value
     */
    private static void handleStringsAndBytes(Document document, String fieldName, Object fieldValue) {
        if (fieldValue instanceof String) {
            document.add(new Field(fieldName, fieldValue.toString(), TextField.TYPE_STORED));
        } else if (fieldValue instanceof byte[]) {
            document.add(new StoredField(fieldName, new BytesRef((byte[]) fieldValue)));
        }
    }

    /**
     * Tries to cast field value (object) to a number (double, integer, float, long).
     * If the field value is not a number then method ignores the field.
     * <p>
     * If the value is a number than the value is warped into a matching lucene field (IntField, DoubleField, ...)
     * and added to the lucene document.
     *
     * @param document   the lucene document to add the number
     * @param fieldName  the field name
     * @param fieldValue the field value
     */
    private static void handleNumbers(Document document, String fieldName, Object fieldValue) {
        if (fieldValue instanceof Double) {
            document.add(new DoubleField(fieldName, Double.parseDouble(fieldValue.toString()), Field.Store.YES));
        } else if (fieldValue instanceof Integer) {
            document.add(new IntField(fieldName, Integer.parseInt(fieldValue.toString()), Field.Store.YES));
        } else if (fieldValue instanceof Float) {
            document.add(new FloatField(fieldName, Float.parseFloat(fieldValue.toString()), Field.Store.YES));
        } else if (fieldValue instanceof Long) {
            document.add(new LongField(fieldName, Long.parseLong(fieldValue.toString()), Field.Store.YES));
        }
    }

}
