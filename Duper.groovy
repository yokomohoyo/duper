#!/usr/bin/env groovy

@Grab("org.codehaus.gpars:gpars:1.2.1")
@Grab('commons-cli:commons-cli:1.4')

import groovyx.gpars.AsyncFun
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest

import static groovyx.gpars.GParsPool.withPool

/**
 *  This object hunts down duplicate files
 * @author Phil <phil@cabro.org>
 */

Integer DEFAULT_MAX_THREADS = 10
Boolean VERBOSE = false

Options options = new Options()
options.addOption("n", true, "Needle file")
options.addOption("h", true, "Haystack directory")
options.addOption("s", true, "Scan directory for duplicates")
options.addOption("t", true, "Threads to use (DEFAULT: 10")
options.addOption("v", false, "Verbosity")

CommandLineParser parser = new DefaultParser()
CommandLine cli = parser.parse(options, args)

if (cli.hasOption("t")) {
    DEFAULT_MAX_THREADS = Integer.valueOf(cli.getOptionValue("t"))
    if (VERBOSE) println "Using ${DEFAULT_MAX_THREADS} thread(s)"
}

if (cli.hasOption("s")) {
    File scanDir = new File(cli.getOptionValue("s"))

    if (scanDir.isDirectory()) {
        TreeSet<String> d = new TreeSet<String>()
        Set<String> dupeChecker = Collections.synchronizedSet(d)
        println "Scanning ${scanDir} for duplicates ..."
        println "---"

        withPool(DEFAULT_MAX_THREADS) {
            Files.walkFileTree(scanDir.toPath(), new SimpleFileVisitor<Path>() {
                @AsyncFun
                def visitor = { File f ->
                    final String hash = MessageDigest.getInstance("MD5").digest(f.getBytes()).encodeHex().toString()
                    final String path = f.absolutePath
                    if (!dupeChecker.add(hash)) {
                        // This is a dupe according to
                        // @link https://java.doc.the.path.to.set add method
                        println path
                    }
                }

                @Override
                FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    visitor(file.toFile())
                    return FileVisitResult.CONTINUE
                }

                @Override
                FileVisitResult visitFileFailed(Path file, IOException e) {
                    println(e)
                    return FileVisitResult.CONTINUE
                }
            })
        }
    } else {
        println "Target is not a directory"
    }
}

if (cli.hasOption("h") && cli.hasOption("n")) {
    File needle = new File(cli.getOptionValue("n"))
    File haystack = new File(cli.getOptionValue("h"))
    Integer totalFiles = 0
    Integer hitCount = 0

    if (!needle.isFile() || !haystack.isDirectory()) {
        println "println usage: 'duper -n <needle> -h <haystack>'"
        System.exit(0)
    }

    if (needle.size() > 0) {
        final String hash = MessageDigest.getInstance("MD5").digest(needle.getBytes()).encodeHex().toString()
        final Integer fileSize = needle.size()
        if (haystack.isDirectory()) {
            withPool(DEFAULT_MAX_THREADS) {
                Files.walkFileTree(haystack.toPath(), new SimpleFileVisitor<Path>() {
                    @AsyncFun
                    def visitor = { File f ->
                        if (fileSize == f.size()) {
                            String fHash = MessageDigest.getInstance("MD5").digest(f.getBytes()).encodeHex().toString()
                            if (hash == fHash) {
                                hitCount++
                                println f.absolutePath
                            }
                        }
                        totalFiles++
                    }

                    @Override
                    FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        visitor(file.toFile())
                        return FileVisitResult.CONTINUE
                    }

                    @Override
                    FileVisitResult visitFileFailed(Path file, IOException e) {
                        println(e)
                        return FileVisitResult.CONTINUE
                    }
                })
            }
            println "Total files scanned: ${totalFiles} ( ${hitCount} dupes )"
        } else {
            println "haystack is not a directory"
        }
    } else {
        println "Needle isn't a file."
    }
}
