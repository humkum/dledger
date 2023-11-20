/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.DLedgerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class DLedgerUtils {
    private static Logger logger = LoggerFactory.getLogger(DLedgerUtils.class);
    public static void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (Throwable ignored) {

        }
    }

    public static long elapsed(long start) {
        return System.currentTimeMillis() - start;
    }

    public static String offset2FileName(final long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long computeEclipseTimeMilliseconds(final long beginTime) {
        return System.currentTimeMillis() - beginTime;
    }

    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty()) {
            return -1;
        }

        try {
            File file = new File(path);

            if (!file.exists()) {
                return -1;
            }

            long totalSpace = file.getTotalSpace();

            if (totalSpace > 0) {
                long usedSpace = totalSpace - file.getFreeSpace();
                long usableSpace = file.getUsableSpace();
                long entireSpace = usedSpace + usableSpace;
                long roundNum = 0;
                if (usedSpace * 100 % entireSpace != 0) {
                    roundNum = 1;
                }
                long result = usedSpace * 100 / entireSpace + roundNum;
                return result / 100.0;
            }
        } catch (Exception e) {
            return -1;
        }
        return -1;
    }

    public static boolean isPathExists(final String path) {
        File file = new File(path);
        return file.exists();
    }


    public static boolean checkDiskViaReadOnly(Path path) {
        try {
            FileStore fileStore = Files.getFileStore(path);
            //fileStore.isReadOnly() true : Disk is failed!
            //fileStore.isReadOnly() false : Disk is healthy!
            return fileStore.isReadOnly();
        } catch (Exception e) {
            logger.warn("Use the read-only permission to determine that the disk write detection method is abnormal", e);
            return false;
        }
    }

    public static boolean checkDiskViaTempFile(Path path) {
        try {
            String tempFileName = "dledger_temp_file" + System.currentTimeMillis();
            File tempFile = new File(path.toFile(), tempFileName);
            boolean created = tempFile.createNewFile();

            if (created) {
                tempFile.delete();
            }
            return created;
        } catch (IOException e) {
            logger.warn("Create a temporary file failed in check disk available", e);
            return false;
        }
    }

    public static boolean diskNotAvailable(DLedgerConfig dLedgerConfig) {
        Set<String> paths = new HashSet<>();
        paths.add(dLedgerConfig.getDefaultPath());
        paths.addAll(Arrays.asList(dLedgerConfig.getDataStorePath().trim().split(DLedgerConfig.MULTI_PATH_SPLITTER)));
        if (dLedgerConfig.getReadOnlyDataStoreDirs() != null) {
            paths.addAll(Arrays.asList(dLedgerConfig.getReadOnlyDataStoreDirs().trim().split(DLedgerConfig.MULTI_PATH_SPLITTER)));
        }

        for (String path : paths) {
            try {
                String diskPath = String.valueOf(Paths.get(path).getParent());
                Path checkPath = Paths.get(diskPath);
                if (!Files.exists(checkPath)) {
                    try {
                        Files.createDirectories(checkPath);
                    } catch (Exception e) {
                        logger.warn("Failed to create the initial directory", e);
                    }
                }
                if (!checkDiskViaTempFile(checkPath) || checkDiskViaReadOnly(checkPath)) {
                    return true;
                }
            } catch (Exception e) {
                logger.error("The disk may be abnormal", e);
            }
        }
        return false;
    }
}
