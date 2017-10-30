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
package de.qaware.chronix.lucene.client

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.apache.lucene.store.FSDirectory
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.BinaryOperator
import java.util.function.Function
/**
 * Unit test for the Chronix Lucene Storage
 * @author f.lautenschlager
 */
class ChronixLuceneStorageTest extends Specification {

    @Shared
    def analyzer = new StandardAnalyzer()

    @Shared
    def group = new Function() {
        @Override
        Object apply(Object o) {
            return o
        }
    }
    @Shared
    def reduce = new Function<Document, Document>() {
        @Override
        Document apply(Document indexableFields) {
            return indexableFields
        }
    } as BinaryOperator


    def "test add and stream document"() {
        given:
        Path path = Paths.get("build/lucene")
        def directory = FSDirectory.open(path)
        def luceneIndex = new LuceneIndex(directory, analyzer)
        def luceneStorage = new ChronixLuceneStorage<>(200, group, reduce)
        def query = createQuery("text")

        luceneIndex.getOpenWriter().deleteAll()

        when:
        def documents = createDocument()
        luceneStorage.add(new SimpleTimeSeriesConverter(), documents, luceneIndex)

        def stream = luceneStorage.stream(new SimpleTimeSeriesConverter(), luceneIndex, query)

        luceneIndex.getOpenReader().close()
        luceneIndex.getDirectory().close()

        then:
        stream.count() == 1
    }

    Collection<SimpleTimeSeries> createDocument() {
        def text = "This is the text to be indexed."

        def document = new SimpleTimeSeries()
        document.add("fieldname", text)

        [document]
    }


    Query createQuery(String searchString) {
        QueryParser queryParser = new QueryParser("fieldname", analyzer)
        return queryParser.parse(searchString)
    }
}
