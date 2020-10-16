package com.datapps.linkoopdb.worker.spi.util.lock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;

public class HDFSExceptionProcessor {

    public static void processInterrupt(IOException ioException) throws IOException {
        if (ioException instanceof InterruptedIOException) {
            Thread.currentThread().interrupt();
        } else if (ioException.getCause() instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw ioException;
        }
    }

    public static void processCreateFileException(IOException ioException) throws IOException {
        if (ioException instanceof RemoteException) {
            if ("org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException".equals(((RemoteException) ioException).getClassName())) {
                //
            } else {
                throw ioException;
            }
        } else if (ioException instanceof FileAlreadyExistsException) {
            //
        } else if (ioException instanceof AlreadyBeingCreatedException) {
            //
        } else {
            throw ioException;
        }
    }

    public static void processReadFileException(IOException ioException) throws IOException {
        if (ioException instanceof RemoteException) {
            if ("java.io.FileNotFoundException".equals(((RemoteException) ioException).getClassName())) {
                //
            } else {
                throw ioException;
            }
        } else if (ioException instanceof FileNotFoundException) {
            //
        } else {
            throw ioException;
        }
    }

}
