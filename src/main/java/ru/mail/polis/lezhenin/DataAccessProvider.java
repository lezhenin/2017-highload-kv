package ru.mail.polis.lezhenin;

import java.io.IOException;
import java.util.NoSuchElementException;

public interface DataAccessProvider {

    void putData(String id, byte [] data) throws IOException;
    byte [] getData(String id) throws IOException, NoSuchElementException;
    void deleteData(String id);

}
