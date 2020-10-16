package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * SimpleStringReader.
 */
public class SimpleReader {

    private transient Charset charset;

    private transient FSDataInputStream inputStream;

    private transient BufferedReader reader;

    /**
     * Creates a new {@code SimpleStringReader} that uses {@code "UTF-8"} charset to convert
     * bytes to strings.
     */
    public SimpleReader() {
    }

    public void open(FileSystem fs, Path path) throws IOException {
        if (inputStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }
        inputStream = fs.open(path);
        reader = new BufferedReader(new InputStreamReader(inputStream));
        this.charset = StandardCharsets.UTF_8;
    }

    public String readLine() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            return new String(line.getBytes(), charset);
        }
        return null;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
        }
    }

}
