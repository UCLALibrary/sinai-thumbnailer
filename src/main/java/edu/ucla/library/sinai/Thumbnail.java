
package edu.ucla.library.sinai;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import info.freelibrary.iiif.image.Image;
import info.freelibrary.iiif.image.ImageFactory;
import info.freelibrary.iiif.image.api.Format;
import info.freelibrary.iiif.image.api.InvalidSizeException;
import info.freelibrary.iiif.image.api.Request;
import info.freelibrary.iiif.image.api.Size;
import info.freelibrary.pairtree.PairtreeFactory;
import info.freelibrary.pairtree.PairtreeFactory.PairtreeImpl;
import info.freelibrary.pairtree.PairtreeObject;
import info.freelibrary.pairtree.PairtreeRoot;
import info.freelibrary.util.FileExtFileFilter;
import info.freelibrary.util.FileUtils;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

import edu.ucla.library.sinai.thumbnailer.MessageCodes;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.vertx.core.Vertx;

/**
 * A command line program to thumbnail TIFF images and upload them up to an S3 Pairtree.
 */
@Command(name = "thumbnail", description = "A thumbnail from CSV generator")
public final class Thumbnail {

    private static final Logger LOGGER = LoggerFactory.getLogger(Thumbnail.class, "thumbnailer_messages");

    private static final String DEFAULT_REGION = "us-east-1";

    @Inject
    public HelpOption myHelpOption;

    @Option(name = { "-d", "--dir" }, description = "A directory or file that contains CSV sources")
    public File mySourceCSVs;

    @Option(name = { "-s", "--size" }, description = "A IIIF-formatted size string (e.g. '200,')")
    public String mySize;

    @Option(name = { "-c", "--csv" }, description = "A file of completed thumbnails")
    public File myCompleted;

    @Option(name = { "-b", "--bucket" }, description = "An S3 bucket into which to store thumbnails")
    public String myBucket;

    @Option(name = { "-r", "--region" }, description = "The region of the supplied S3 bucket (if not us-east-1)")
    public String myRegion;

    @Option(name = { "-p", "--profile" }, description = "An AWS profile which has permissions to the S3 bucket")
    public String myProfile;

    private boolean isTrackingIDs;

    private final Vertx myVertx = Vertx.vertx();

    /**
     * The main method for the thumbnail program.
     *
     * @param args Arguments supplied to the program
     */
    @SuppressWarnings("uncommentedmain")
    public static void main(final String[] args) {
        final Thumbnail thumbnail = SingleCommand.singleCommand(Thumbnail.class).parse(args);

        if (thumbnail.myHelpOption.showHelpIfRequested()) {
            return;
        }

        thumbnail.run();
    }

    private void run() {
        if (mySourceCSVs == null) {
            LOGGER.error(MessageCodes.T_002);
        } else if (mySize == null) {
            LOGGER.error(MessageCodes.T_006, "null");
        } else if (myBucket == null) {
            LOGGER.error(MessageCodes.T_009);
        } else {
            final PairtreeFactory ptFactory = PairtreeFactory.getFactory(myVertx, PairtreeImpl.S3Bucket);
            final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            final List<String[]> images = new ArrayList<>();
            final Set<String> completed = new HashSet<>();
            final ProfileCredentialsProvider credProvider;
            final String region;
            final AmazonS3 s3;

            CSVWriter writer = null;
            CSVReader reader;

            // Set credentials profile to default or supplied
            if (myProfile != null) {
                credProvider = new ProfileCredentialsProvider(myProfile);
            } else {
                credProvider = new ProfileCredentialsProvider();
            }

            // Set S3 region to default or supplied
            if (myRegion != null) {
                region = myRegion;
            } else {
                region = DEFAULT_REGION;
            }

            // Create our S3 client
            s3 = builder.withCredentials(credProvider).withRegion(region).build();

            try {
                // If we're given a file for completed thumbnails, keep track
                if (myCompleted != null) {
                    writer = new CSVWriter(new FileWriter(myCompleted, true));
                    isTrackingIDs = true;

                    // Add images we've already converted to a skip list
                    if (myCompleted.exists() && (myCompleted.length() > 0)) {
                        LOGGER.info(MessageCodes.T_007);

                        reader = new CSVReader(new FileReader(myCompleted));

                        reader.forEach(id -> {
                            completed.add(id[0]);
                        });

                        reader.close();
                    }
                }

                // Gather a list of all the images
                for (final File file : FileUtils.listFiles(mySourceCSVs, new FileExtFileFilter("csv"), true)) {
                    reader = new CSVReader(new FileReader(file));

                    images.addAll(reader.readAll());
                    reader.close();
                }

                // Create thumbnails for the images
                for (final String[] imageInfo : images) {
                    final String id = imageInfo[0];

                    // Create thumbnail if it's not in the skip list
                    if (!completed.contains(id)) {
                        final String path = imageInfo[1];

                        LOGGER.info(MessageCodes.T_005, path, id);

                        final Request request = new Request(id, "iiif", Size.parse(mySize));
                        final Image image = ImageFactory.getImage(new File(path));
                        final AWSCredentials credentials = credProvider.getCredentials();
                        final String accessKey = credentials.getAWSAccessKeyId();
                        final String secretKey = credentials.getAWSSecretKey();
                        final PairtreeRoot ptRoot = ptFactory.getPairtree(myBucket, accessKey, secretKey);
                        final PairtreeObject ptObject = ptRoot.getObject(request.getID());
                        final String key = ptObject.getPath(request.getPath());
                        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                        final ObjectMetadata metadata = new ObjectMetadata();
                        final PutObjectRequest put;
                        final byte[] bytes;

                        // Create the thumbnail
                        image.transform(request).write(Format.JPG, byteStream);
                        bytes = byteStream.toByteArray();

                        // Configure the PUT request's metadata
                        metadata.setContentLength(bytes.length);
                        metadata.setContentType(Format.JPG.getMimeType());

                        // Create the PUT request
                        put = new PutObjectRequest(myBucket, key, new ByteArrayInputStream(bytes), metadata);
                        put.setCannedAcl(CannedAccessControlList.PublicRead);

                        // PUT the thumbnail to S3
                        s3.putObject(put);

                        // Write the thumbnail'ed ID if requested
                        if (isTrackingIDs) {
                            writer.writeNext(new String[] { id }, true);
                            writer.flush();
                        }
                    } else if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(MessageCodes.T_008, id);
                    }
                }
            } catch (final FileNotFoundException details) {
                LOGGER.error(MessageCodes.T_003, details.getMessage());
            } catch (final IOException details) {
                LOGGER.error(MessageCodes.T_004, details.getMessage());
            } catch (final InvalidSizeException details) {
                LOGGER.error(MessageCodes.T_006, mySize);
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (final IOException details) {
                        LOGGER.error(details.getMessage(), details);
                    }
                }
            }
        }
    }
}
