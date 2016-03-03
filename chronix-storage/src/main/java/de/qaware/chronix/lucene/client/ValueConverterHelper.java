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

import org.apache.lucene.document.*;
import org.apache.lucene.util.BytesRef;

import java.util.Collection;
import java.util.Map;

/**
 * Created by f.lautenschlager on 03.03.2016.
 */
public class ValueConverterHelper {


    public static final String MULTI_VALUE_FIELD_DELIMITER = "::mv::";

    private ValueConverterHelper() {

    }

    public static void handleCollectionsAndArrays(Document document, Map.Entry<String, Object> entry) {
        if (entry.getValue() instanceof Collection) {
            Collection objects = (Collection) entry.getValue();

            int fieldCounter = 0;
            for (Object o : objects) {
                fieldCounter++;
                String key = entry.getKey() + MULTI_VALUE_FIELD_DELIMITER + fieldCounter;

                handleNumbers(document, key, o);
                handleStringsAndBytes(document, key, o);
            }
        }
    }

    public static void handleStringsAndBytes(Document document, String key, Object value) {
        if (value instanceof String) {
            document.add(new Field(key, value.toString(), TextField.TYPE_STORED));
        } else if (value instanceof byte[]) {
            document.add(new StoredField(key, new BytesRef((byte[]) value)));
        }
    }

    public static void handleNumbers(Document document, String key, Object value) {
        if (value instanceof Double) {
            document.add(new DoubleField(key, Double.parseDouble(value.toString()), Field.Store.YES));
        } else if (value instanceof Integer) {
            document.add(new IntField(key, Integer.parseInt(value.toString()), Field.Store.YES));
        } else if (value instanceof Float) {
            document.add(new FloatField(key, Float.parseFloat(value.toString()), Field.Store.YES));
        } else if (value instanceof Long) {
            document.add(new LongField(key, Long.parseLong(value.toString()), Field.Store.YES));
        }
    }
}
