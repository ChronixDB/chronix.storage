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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class that holds the lucene index writer and searcher
 *
 * @author f.lautenschlager
 */
public final class LuceneIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndex.class);

    private IndexSearcher searcher;
    private IndexReader reader;
    private IndexWriter writer;

    private final Directory directory;
    private final Analyzer analyzer;

    /**
     * Constructs and lucene index
     *
     * @param directory the directory of the index (RAM, File System, ...)
     * @param analyzer  the analyzer for the reader and writer
     */
    public LuceneIndex(Directory directory, Analyzer analyzer) {
        this.directory = directory;
        this.analyzer = analyzer;
    }

    /**
     * Initializes the searcher, if not initialized.
     * Closes the writer and opens a searcher.
     *
     * @return the lucene index searcher
     * @throws IOException if the underlying lucene reader can not be opened or created
     */
    public IndexSearcher getSearcher() throws IOException {
        if (searcher == null && readerClosed()) {
            reader = getOpenReader();
            searcher = new IndexSearcher(reader);
        }
        return searcher;

    }


    /**
     * This method returns an open writer for the given directory.
     * If the reader is open this method will close the reader.
     *
     * @return an open lucene writer
     * @throws IOException if the lucene writer can not be opened or created
     */
    public IndexWriter getOpenWriter() throws IOException {
        if (writerClosed()) {
            LOGGER.debug("Closing reader and opening writer.");
            if (readerOpen()) {
                LOGGER.debug("Closing reader.");
                reader.close();
            }
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            writer = new IndexWriter(directory, config);
        }
        return writer;
    }

    /**
     * Closes the index writer if it is open.
     * Then opens the index reader.
     *
     * @return an open lucene reader.
     * @throws IOException if the lucene reader can not be opened or created
     */
    public IndexReader getOpenReader() throws IOException {
        if (writerOpen()) {
            LOGGER.debug("Closing writer");
            writer.close();
        }
        if (readerClosed()) {
            LOGGER.debug("Opening reader");
            reader = DirectoryReader.open(directory);
        }
        return reader;
    }


    /**
     * @return the directory holding the index
     */
    public Directory getDirectory() {
        return directory;
    }

    /**
     * @return true if the reader is open
     */
    private boolean readerOpen() {
        return !readerClosed();
    }

    /**
     * @return true if the reader is null or closed
     */
    private boolean readerClosed() {
        return reader == null || reader.getRefCount() == 0;
    }

    /**
     * @return true if the writer is null or closed
     */
    private boolean writerClosed() {
        return writer == null || !writer.isOpen();
    }

    /**
     * @return true if the writer is open
     */
    private boolean writerOpen() {
        return !writerClosed();
    }
}
