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

import java.util.HashMap;
import java.util.Map;

/**
 * An simple time series (without points ;-) used for test purposes
 *
 * @author f.lautenschlager
 */
public class SimpleTimeSeries {

    private Map<String, Object> fields;

    /**
     * Constructs a simple time series
     */
    public SimpleTimeSeries() {
        fields = new HashMap<>();
    }

    /**
     * Adds a field for the given name and value.
     * Overrides old values.
     *
     * @param name  the field name
     * @param value the field value
     */
    public void add(String name, Object value) {
        fields.put(name, value);
    }

    /**
     * @return the fields of the time series
     */
    public Map<String, Object> getFields() {
        return fields;
    }
}
