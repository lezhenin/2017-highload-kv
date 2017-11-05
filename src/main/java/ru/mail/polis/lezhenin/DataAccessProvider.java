package ru.mail.polis.lezhenin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;


public interface DataAccessProvider {

    void putData(@NotNull String id, byte [] data) throws IOException, IllegalArgumentException;
    byte [] getData(@NotNull String id) throws IOException, IllegalArgumentException, NoSuchElementException;
    void deleteData(@NotNull String id) throws IOException, IllegalArgumentException;
    boolean isExist(@NotNull String id) throws IOException, IllegalArgumentException;
    boolean isDeleted(@NotNull String id) throws IOException, IllegalArgumentException;

}
