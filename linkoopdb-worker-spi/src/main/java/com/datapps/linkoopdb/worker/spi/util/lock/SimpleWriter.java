package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * SimpleStringReader.
 */
public class SimpleWriter {

    private transient Charset charset;

    private transient FSDataOutputStream outputStream;

    private transient Path path;
    private transient FileSystem fileSystem;

    /**
     * Creates a new {@code SimpleStringReader} that uses {@code "UTF-8"} charset to convert
     * bytes to strings.
     */
    public SimpleWriter() {
    }

    public void open(FileSystem fs, Path path) throws IOException {
        if (outputStream != null) {
            throw new IllegalStateException("Writer has already been opened");
        }
        this.charset = StandardCharsets.UTF_8;
        try {
            this.outputStream = fs.create(path, false);
        } catch (IOException ioException) {
            HDFSExceptionProcessor.processCreateFileException(ioException);
            outputStream = fs.append(path);
        }
    }

    public void open(FileSystem fs, FSDataOutputStream outputStream, Path path) {
        this.fileSystem = fs;
        this.path = path;
        Objects.requireNonNull(outputStream);
        this.outputStream = outputStream;
        this.charset = StandardCharsets.UTF_8;
    }

    public void writeString(String str) throws IOException {
        FSDataOutputStream outputStream = getStream();
        outputStream.write(str.getBytes(charset));
        outputStream.write('\n');
    }

    public long hflush() throws IOException {
        if (outputStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        outputStream.hflush();
        return outputStream.getPos();
    }

    public long hsync() throws IOException {
        if (outputStream == null) {
            throw new IllegalStateException("Writer is not open");
        }
        outputStream.hsync();
        return outputStream.getPos();
    }

    /**
     * Returns the current output stream, if the stream is open.
     */
    protected FSDataOutputStream getStream() {
        if (outputStream == null) {
            throw new IllegalStateException("Input stream has not been opened");
        }
        return outputStream;
    }

    public void close() throws IOException {
        try {
            if (outputStream != null) {
                outputStream.close();
                outputStream = null;
            }
        } catch (Exception e) {
            if (fileSystem instanceof DistributedFileSystem) {
                LeaseManger.recoverLease((DistributedFileSystem) fileSystem, path);
            }
        }
    }

}
