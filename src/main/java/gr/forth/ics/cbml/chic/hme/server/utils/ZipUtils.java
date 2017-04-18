package gr.forth.ics.cbml.chic.hme.server.utils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Created by ssfak on 7/1/16.
 */
public class ZipUtils {
    public static boolean isZipFile(final Path file) {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
            boolean isZip = in.readInt() == 0x504b0304;
            return isZip;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }

    }

    public static void zcat(InputStream zipIn, OutputStream out) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(zipIn)) {
            final int MAX_BUF = 4 * 1024 * 1024;
            byte buffer[] = new byte[MAX_BUF];
            for (ZipEntry entry = zis.getNextEntry(); entry != null; entry = zis.getNextEntry()) {
                while (true) {
                    int k = zis.read(buffer, 0, MAX_BUF);
                    if (k > 0)
                        out.write(buffer, 0, k);
                    else
                        break;
                }
            }
        }
    }

    public static void unzipFile(Path outputDir, File zipFile) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath()))) {
            final int MAX_BUF = 4 * 1024 * 1024;
            byte buffer[] = new byte[MAX_BUF];
            for (ZipEntry entry = zis.getNextEntry(); entry != null; entry = zis.getNextEntry()) {
                final Path outputEntryPath = Paths.get(outputDir.toString(), entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(outputEntryPath);
                } else {
                    if (!Files.exists(outputEntryPath.getParent())) {
                        Files.createDirectories(outputEntryPath.getParent());
                    }
                    try (OutputStream fos = Files.newOutputStream(outputEntryPath)) {
                        while (true) {
                            int k = zis.read(buffer, 0, MAX_BUF);
                            if (k > 0)
                                fos.write(buffer, 0, k);
                            else
                                break;
                        }
                    }
                    // System.out.println("==> wrote " + entry.getName() + " at " + outputEntryPath);
                }
                zis.closeEntry();
            }
        }
    }

    public static void zipDir(final Path folder, final Path zipFilePath) throws IOException {
        try (OutputStream fos = Files.newOutputStream(zipFilePath);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    zos.putNextEntry(new ZipEntry(folder.relativize(file).toString()));
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    zos.putNextEntry(new ZipEntry(folder.relativize(dir).toString() + "/"));
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    static void visitEntries(final Path zipPath, FileVisitor<Path> visitor) throws IOException {
        try (FileSystem fs = FileSystems.newFileSystem(new URI("jar", zipPath.toUri().toString(), null), Collections.singletonMap("create", "false"), null)) {
            fs.getRootDirectories().forEach(root -> {
                try {
                    Files.walkFileTree(root, visitor);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
