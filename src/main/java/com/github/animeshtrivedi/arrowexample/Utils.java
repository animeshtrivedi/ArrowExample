package com.github.animeshtrivedi.arrowexample;

import java.io.File;

/**
 * Created by atr on 20.12.17.
 */
public class Utils {
    static File validateFile(String fileName, boolean shouldExist) {
        if (fileName == null) {
            throw new IllegalArgumentException("missing file parameter");
        }
        File f = new File(fileName);
        if (shouldExist && (!f.exists() || f.isDirectory())) {
            throw new IllegalArgumentException(fileName + " file not found: " + f.getAbsolutePath());
        }
        if (!shouldExist && f.exists()) {
            throw new IllegalArgumentException(fileName + " file already exists: " + f.getAbsolutePath());
        }
        return f;
    }
}
