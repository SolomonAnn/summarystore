package com.samsung.sra.experiments.iotdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataReader {
    private static final Logger logger = LoggerFactory.getLogger(DataReader.class);

    private final String fileName;

    public DataReader(String fileName) {
        this.fileName = fileName;
    }

    public List<String> readData() {
        List<String> data = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                data.add(line);
            }
            reader.close();
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
        return data;
    }
}
