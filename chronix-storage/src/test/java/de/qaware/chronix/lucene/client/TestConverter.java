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

import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;

/**
 * A converter used for test purposes
 *
 * @author f.lautenschlager
 */
public class TestConverter implements TimeSeriesConverter<MySimpleTimeSeries> {
    @Override
    public MySimpleTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        MySimpleTimeSeries doc = new MySimpleTimeSeries();
        binaryTimeSeries.getFields().forEach(doc::add);

        return doc;
    }

    @Override
    public BinaryTimeSeries to(MySimpleTimeSeries document) {
        BinaryTimeSeries.Builder binaryTimeSeries = new BinaryTimeSeries.Builder();
        document.getFields().forEach(binaryTimeSeries::field);
        return binaryTimeSeries.build();
    }
}
