/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * It is used for test with local file system.
 * Hadoop default local file system is not appendable so we here create an appendable local file system
 */
public class LinkoopDBLocalFileSystem extends RawLocalFileSystem {

    static final URI uri = URI.create("ldb:///");

    @Override
    public String getScheme() {
        return "ldb";
    }


    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    protected void checkPath(Path path) {
        //        super.checkPath(path);
    }

    @Override
    public File pathToFile(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(getWorkingDirectory(), path);
        }
        return new File(path.toUri().getPath().replace("ldb:", "file:"));
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        File file = pathToFile(f);
        java.nio.file.Path path = Paths.get(file.getAbsolutePath());

        if (!file.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }

        if (path.getFileSystem().provider().getClass().getName().contains("WindowsFileSystemProvider")) {
            BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
            return new FileStatus(attr.size(), attr.isDirectory(), 1,
                getDefaultBlockSize(f), attr.lastModifiedTime().toMillis(), f);
        } else {
            PosixFileAttributes attr = Files.readAttributes(path, PosixFileAttributes.class);
            return new FileStatus(attr.size(), attr.isDirectory(), 1,
                getDefaultBlockSize(f), attr.lastModifiedTime().toMillis(),
                attr.lastAccessTime().toMillis(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
                attr.owner().getName(), attr.group().getName(), f);
        }

    }
}
