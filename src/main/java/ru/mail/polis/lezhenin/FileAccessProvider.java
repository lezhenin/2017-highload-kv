package ru.mail.polis.lezhenin;


import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileAccessProvider implements DataAccessProvider {

    private final static String VALID_FILENAME_REGEX = "[.\\-_a-zA-Z0-9]+";
    private final static String DELETED_ID_FILE_NAME = "/deleted.txt";

    private final File storageDir;
    private final Set<String> deletedEntries = new HashSet<>();

    public FileAccessProvider(File storageDir) {
        this.storageDir = storageDir;
        loadState();
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
        if (deletedEntries.contains(id)) {
            deletedEntries.remove(id);
            saveState();
        }
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
        if (!deletedEntries.contains(id)) {
            deletedEntries.add(id);
            saveState();
        }
    }

    @Override
    public boolean doesExist(@NotNull String id) throws IOException, IllegalArgumentException {
        Path path = constructPath(id);
        return Files.exists(path);
    }

    @Override
    public boolean isDeleted(@NotNull String id) throws IOException, IllegalArgumentException {
        return deletedEntries.contains(id);
    }

    private void loadState() {
        try {
            File deletedEntriesListFile = new File(storageDir.getAbsolutePath() + DELETED_ID_FILE_NAME);
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(deletedEntriesListFile));
            int length = inputStream.available();
            byte [] data = new byte[length];
            String ids = new String(data, "UTF-8");
            deletedEntries.addAll(Arrays.asList(ids.split("\\s+")));
        } catch (IOException ignored) { }
    }

    private void saveState() {
        try {
            File deletedEntriesListFile = new File(storageDir.getAbsolutePath() + DELETED_ID_FILE_NAME);
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(deletedEntriesListFile));
            for (String id : deletedEntries)
                outputStream.write((id + " ").getBytes("UTF-8"));
            outputStream.flush();
            outputStream.close();
        } catch (IOException ignored) { }
    }
}
