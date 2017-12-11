
package edu.ucla.library.sinai;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.inject.Inject;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import info.freelibrary.util.FileExtFileFilter;
import info.freelibrary.util.FileUtils;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

import edu.ucla.library.sinai.thumbnailer.MessageCodes;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;

/**
 * A throw-away command line program to reconcile CSV files with the current file system state.
 * <p>
 * To use: <pre>java -cp target/sinai-thumbnailer-0.0.1.jar edu.ucla.library.sinai.Reconciler -h</pre>
 * </p>
 */
@Command(name = "edu.ucla.library.sinai.Reconciler", description = "A file system to CSV reconciler")
public final class Reconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reconciler.class, "thumbnailer_messages");

    @Inject
    public HelpOption myHelpOption;

    @Option(name = { "-d", "--dir" }, description = "A directory that contains CSV source files")
    public File myDir;

    // Generate with: find /sinai/cifsemel -printf '"%P";"%Tc";"%s";\n' > ~/filesystem.csv
    @Option(name = { "-c", "--csv" }, description = "A CSV file from a file system traversal")
    public File myCSVFile;

    @Option(name = { "-p", "--path" },
            description = "The path supplied to the find command used to generate the file system CSV")
    public String myTIFDirPath;

    @Option(name = { "-o", "--output" }, description = "A CSV file with the reconciled file system and IDs")
    public File myOutput;

    /**
     * The main method for the reconciler program.
     *
     * @param args Arguments supplied to the program
     */
    @SuppressWarnings("uncommentedmain")
    public static void main(final String[] args) {
        final Reconciler reconciler = SingleCommand.singleCommand(Reconciler.class).parse(args);

        if (reconciler.myHelpOption.showHelpIfRequested()) {
            return;
        }

        reconciler.run();
    }

    private void run() {
        Objects.requireNonNull(myDir, LOGGER.getMessage(MessageCodes.T_002));
        Objects.requireNonNull(myOutput, LOGGER.getMessage(MessageCodes.T_016));
        Objects.requireNonNull(myCSVFile, LOGGER.getMessage(MessageCodes.T_010));
        Objects.requireNonNull(myTIFDirPath, LOGGER.getMessage(MessageCodes.T_011));

        final List<String[]> fsImages = new ArrayList<>();
        final Map<String, String> csvImages = new HashMap<>();
        final CSVReader fsReader;

        try {
            final CSVParser parser = new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(true).build();

            fsReader = new CSVReaderBuilder(new FileReader(myCSVFile)).withCSVParser(parser).build();
            CSVReader csvReader;

            try {
                // Gather a list of all the images
                for (final File file : FileUtils.listFiles(myDir, new FileExtFileFilter("csv"), true)) {
                    csvReader = new CSVReader(new FileReader(file));

                    try {
                        String[] next;

                        while ((next = csvReader.readNext()) != null) {
                            if (next.length > 1) {
                                final String name = next[1].substring(next[1].lastIndexOf('/') + 1);
                                final String id = next[0];

                                // We have some duplicates across the different CSV versions
                                if (!csvImages.containsKey(name)) {
                                    if (csvImages.put(name, id) != null) {
                                        LOGGER.error(MessageCodes.T_012);
                                    }
                                }
                            }
                        }

                        csvReader.close();
                    } catch (final IOException details) {
                        throw new IOException(file.getAbsolutePath(), details);
                    }
                }

                LOGGER.info(MessageCodes.T_013, csvImages.size());

                // Gather list of files on file system
                try {
                    fsImages.addAll(fsReader.readAll());
                    fsReader.close();
                } catch (final IOException details) {
                    throw new IOException(myCSVFile.getAbsolutePath(), details);
                }

                final Iterator<String[]> iterator = fsImages.iterator();
                final SortedMap<String, String> fsMap = new TreeMap<>();

                while (iterator.hasNext()) {
                    final String name = Paths.get(myTIFDirPath, iterator.next()[0]).toString();

                    // Only pay attention to TIFF images
                    if (name.toLowerCase().endsWith(".tif")) {
                        if (!fsMap.containsKey(name)) {
                            final String id = csvImages.get(name.substring(name.lastIndexOf('/') + 1));

                            if (id != null) {
                                fsMap.put(name, id);
                            }
                        } else {
                            LOGGER.error(MessageCodes.T_015, name);
                        }
                    } // else a non-TIFF file
                }

                LOGGER.info(MessageCodes.T_014, fsMap.size());

                // Write out our reconciled paths and IDs into a CSV file
                final CSVWriter writer = new CSVWriter(new FileWriter(myOutput));
                final Iterator<String> fsIterator = fsMap.keySet().iterator();

                while (fsIterator.hasNext()) {
                    final String key = fsIterator.next();

                    writer.writeNext(new String[] { key, fsMap.get(key) });
                }

                writer.close();
            } catch (final FileNotFoundException details) {
                LOGGER.error(MessageCodes.T_003, myDir);
            } catch (final IOException details) {
                LOGGER.error(MessageCodes.T_004, details.getMessage(), details.getCause());
            }
        } catch (final FileNotFoundException details) {
            LOGGER.error(MessageCodes.T_003, myCSVFile);
        }
    }

}
