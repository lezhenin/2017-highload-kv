package ru.mail.polis.lezhenin;


import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class FileAccessProvider implements DataAccessProvider {

    private final static String VALID_FILENAME_REGEX = "[.\\-_a-zA-Z0-9]+";

    private final File storageDir;
    private final Set<String> deletedEntries = new HashSet<>();

    public FileAccessProvider(File storageDir) {
        this.storageDir = storageDir;
    }

    private Path constructPath(@NotNull String id) throws IllegalArgumentException {
        if (id.isEmpty() || !id.matches(VALID_FILENAME_REGEX)) {
            throw new IllegalArgumentException();
        }
        return Paths.get(storageDir.getPath(), id);
    }

    @Override
    public void putData(@NotNull String id, byte[] data) throws IOException, IllegalArgumentException {
        Path path = constructPath(id);
        Files.write(path, data);
        deletedEntries.remove(id);
    }

    @Override
    public byte[] getData(@NotNull String id) throws IOException, IllegalArgumentException, NoSuchElementException {
        Path path = constructPath(id);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("File with id " + id + " not found");
        }
        return Files.readAllBytes(path);
    }

    @Override
    public void deleteData(@NotNull String id) throws IOException, IllegalArgumentException {
        Path path = constructPath(id);
        Files.deleteIfExists(path);
        deletedEntries.add(id);
    }

    @Override
    public boolean isExist(@NotNull String id) throws IOException, IllegalArgumentException {
        Path path = constructPath(id);
        return Files.exists(path);
    }

    @Override
    public boolean isDeleted(@NotNull String id) throws IOException, IllegalArgumentException {
        return deletedEntries.contains(id);
    }
}
