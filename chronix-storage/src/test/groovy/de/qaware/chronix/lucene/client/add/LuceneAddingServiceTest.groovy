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
package de.qaware.chronix.lucene.client.add

import de.qaware.chronix.lucene.client.LuceneIndex
import de.qaware.chronix.lucene.client.SimpleTimeSeries
import de.qaware.chronix.lucene.client.SimpleTimeSeriesConverter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.IndexableField
import org.apache.lucene.store.RAMDirectory
import spock.lang.Shared
import spock.lang.Specification

/**
 * Unit test for the lucene adding service
 * @author f.lautenschlager
 */
class LuceneAddingServiceTest extends Specification {

    @Shared
    def expectedFields = new ArrayList<IndexableField>();

    def setup() {
        //double array
        expectedFields.add(new StoredField("double_array::mv::1", 5.0d))
        expectedFields.add(new StoredField("double_array::mv::2", 3.2))
        expectedFields.add(new StoredField("double_array::mv::3", 4.2d))

        //Single fields
        expectedFields.add(new StoredField("double", 5.0d))
        expectedFields.add(new StoredField("float", 3.2f))
        expectedFields.add(new StoredField("int", 10))
        expectedFields.add(new StoredField("long", 12l))

        expectedFields.add(new StoredField("string", "hello"))

        //byte field
        expectedFields.add(new StoredField("bytes", "chronix rocks".bytes))

        //String array
        expectedFields.add(new StoredField("string_array::mv::1", "one"))
        expectedFields.add(new StoredField("string_array::mv::2", "two"))
        expectedFields.add(new StoredField("string_array::mv::3", "three"))

        //mixed
        expectedFields.add(new StoredField("mixed::mv::1", "hello"))
        expectedFields.add(new StoredField("mixed::mv::2", 1.2d))
        expectedFields.add(new StoredField("mixed::mv::3", "chronix"))

    }

    def "test add"() {
        given:
        def timeSeries = createTimeSeries(1) as Collection
        def luceneIndex = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer())

        when:
        def result = LuceneAddingService.add(new SimpleTimeSeriesConverter(), timeSeries, luceneIndex.openWriter)
        luceneIndex.openWriter.commit()
        def doc = luceneIndex.searcher.doc(0)

        then:
        result
        def fields = doc.fields
        fields.size() == 15


        checkIfEquals(fields, expectedFields)
    }

    def "test private constructor"() {
        when:
        LuceneAddingService.newInstance()
        then:
        noExceptionThrown()
    }

    def "test with empty or null argument"() {
        when:
        def returned = LuceneAddingService.add(new SimpleTimeSeriesConverter(), collection, null)
        then:
        returned

        where:
        collection << [null, new ArrayList<>()]

    }


    boolean checkIfEquals(List<IndexableField> returned, ArrayList<IndexableField> expected) {
        List<String> returnedAsStrings = new ArrayList<>()
        List<String> expectedAsStrings = new ArrayList<>()

        returned.each {
            returnedAsStrings.add(it.name() + value(it))
        }

        expected.each {
            expectedAsStrings.add(it.name() + value(it))
        }

        returnedAsStrings.containsAll(expectedAsStrings)
    }

    Object value(IndexableField value) {
        if (value.numericValue() != null) {
            return value.numericValue()
        }
        if (value.binaryValue() != null) {
            return value.binaryValue().bytes
        }
        if (value.stringValue() != null) {
            return value.stringValue();
        }
        return "nothing"
    }


    Collection<SimpleTimeSeries> createTimeSeries(int numberOfTimeSeries) {
        def result = new ArrayList<>();

        numberOfTimeSeries.times {
            SimpleTimeSeries sts = new SimpleTimeSeries()
            sts.add("string", "hello")

            sts.add("double", 5.0d)
            sts.add("int", 10i)
            sts.add("float", 3.2f)
            sts.add("long", 12l)

            sts.add("bytes", "chronix rocks".bytes)

            sts.add("double_array", [5.0d, 3.2d, 4.2d] as Double[])
            sts.add("string_array", ["one", "two", "three"] as String[])

            sts.add("mixed", ["hello", 1.2d, "chronix"] as ArrayList)
            //Not a pojo
            sts.add("ignored", new SimpleTimeSeries())


            result.add(sts)
        }
        result
    }

}
