package com.datapps.linkoopdb.worker.spi.fs;

import static com.datapps.linkoopdb.jdbc.persist.LdbSqlDatabasePropertiesConsts.dfs_AbstractFileSystem_http_bufferSize_key;
import static com.datapps.linkoopdb.jdbc.persist.LdbSqlDatabasePropertiesConsts.dfs_AbstractFileSystem_http_bufferSize_value;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

abstract class AbstractHttpFileSystem extends FileSystem {

    private static final long DEFAULT_BLOCK_SIZE = 4096;
    private static final Path WORKING_DIR = new Path("/");
    private int MAX_BUFFER_SIZE = dfs_AbstractFileSystem_http_bufferSize_value;

    private URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = name;
        this.MAX_BUFFER_SIZE = super.getConf().getInt(dfs_AbstractFileSystem_http_bufferSize_key, dfs_AbstractFileSystem_http_bufferSize_value);
    }

    public abstract String getScheme();

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        URLConnection conn = path.toUri().toURL().openConnection();
        conn.setRequestProperty("Range", "bytes=0-" + MAX_BUFFER_SIZE);
        InputStream in = conn.getInputStream();
        long length = head(path).getContentLengthLong();
        return new FSDataInputStream(new HttpDataInputStream(in, uri, MAX_BUFFER_SIZE, length));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
        boolean b, int i, short i1, long l,
        Progressable progressable)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return new FileStatus[]{getFileStatus(path)};
    }

    @Override
    public Path getWorkingDirectory() {
        return WORKING_DIR;
    }

    @Override
    public void setWorkingDirectory(Path path) {
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
        throws IOException {
        return false;
    }

    private HttpURLConnection get(Path path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) path.toUri().toURL().openConnection();
        conn.setRequestMethod("GET");
        return conn;
    }

    private HttpURLConnection head(Path path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) path.toUri().toURL().openConnection();
        conn.setRequestMethod("HEAD");
        return conn;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        URLConnection conn = head(path);
        return new FileStatus(conn.getContentLengthLong(), false, 1, DEFAULT_BLOCK_SIZE, 0, path);
    }

    private static class HttpDataInputStream extends FilterInputStream
        implements Seekable, PositionedReadable {

        private long position = 0L;
        private URI uri;
        private int bufferSize;
        private long length;

        HttpDataInputStream(InputStream in, URI uri, int bufferSize, long length) {
            super(in);
            this.uri = uri;
            this.bufferSize = bufferSize;
            this.length = length;
        }

        private void allocateNewStream(long targetPosition) throws IOException {
            allocateNewStream(targetPosition, bufferSize);
        }

        private void allocateNewStream(long targetPosition, int len) throws IOException {
            long batchSize = len < bufferSize ? bufferSize : len;
            try {
                // 关闭旧的stream
                super.in.close();
                // 根据seek的位置申请新的stream
                URLConnection conn = uri.toURL().openConnection();
                conn.setRequestProperty("Range", "bytes=" + targetPosition + "-" + (targetPosition + batchSize));
                super.in = conn.getInputStream();
                this.position = targetPosition;
            } catch (IOException e) {
                System.out.println("pos: " + this.position + "batchSize: " + batchSize);
                throw e;
            }
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
            seek(position);
            return read(buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
            seek(position);
            IOUtils.readFully(this, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            seek(position);
            IOUtils.readFully(this, buffer, 0, buffer.length);
        }

        @Override
        public void seek(long pos) throws IOException {
            long positionOffset = pos - this.position;
            if (positionOffset < 0 || positionOffset >= bufferSize) {
                allocateNewStream(pos);
            } else {
                long skip = super.in.skip(positionOffset);
                if (skip != positionOffset) {
                    allocateNewStream(pos);
                } else {
                    this.position = pos;
                }
            }
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            allocateNewStream(position);
            return true;
        }

        @Override
        public int read() throws IOException {
            if (in.available() == 0 && this.position < this.length) {
                allocateNewStream(this.position);
            }
            this.position++;
            return super.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (in.available() < len && this.position < this.length) {
                allocateNewStream(this.position, len);
            }
            int read = super.read(b, off, len);
            if (read != -1) {
                this.position += read;
            }
            return read;
        }

        @Override
        public long skip(long n) throws IOException {
            long skip = in.skip(n);
            this.position += skip;
            return skip;
        }
    }
}
