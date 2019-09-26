package com.kenshoo.mcdownload.services;

import org.jooq.lambda.Seq;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class StringIteratorInputStream extends InputStream {

    private final AutoCloseable toClose;
    private final Iterator<Byte> bytes;
    private long readCount = 0;

    public StringIteratorInputStream(Stream<String> strings) {
        this.toClose = strings;
        this.bytes = strings.flatMap(this::bytesOf).iterator();
    }

    @Override
    public int read() throws IOException {
        if (bytes.hasNext()) {
            readCount++;
            return bytes.next();
        } else {
            return -1;
        }
    }

    private Stream<Byte> bytesOf(String s) {
        byte[] bytes = s.getBytes();
        return Seq.iterate(0, i -> i + 1)
                .limitWhile(i -> i < s.length())
                .map(i -> bytes[i]);
    }

    @Override
    public void close() throws IOException {
        try {
            toClose.close();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    public long getReadCount() {
        return readCount;
    }

}
