package ru.mail.polis.lezhenin;


import java.io.*;
import java.util.NoSuchElementException;

public class FileAccessProvider implements DataAccessProvider {

    private final File storage;

    FileAccessProvider(File storage) {
        this.storage = storage;
    }

    @Override
    public void putData(String id, byte[] data) throws IOException {
        File entry = new File(storage.getPath() + '/' + id);
        FileOutputStream stream = new FileOutputStream(entry, false);
        stream.write(data);
        stream.close();
    }

    @Override
    public byte[] getData(String id) throws IOException, NoSuchElementException {
        File entry = new File(storage.getPath() + '/' + id);
        if (!entry.exists()) {
            throw new NoSuchElementException("File with id " + id + " not found");
        }
        FileInputStream stream = new FileInputStream(entry);
        int length = stream.available();
        byte [] data = new byte[length];
        stream.read(data);
        stream.close();
        return data;
    }

    @Override
    public void deleteData(String id) {
        File entry = new File(storage.getPath() + '/' + id);
        if (entry.exists()) {
            entry.delete();
        }
    }
}
