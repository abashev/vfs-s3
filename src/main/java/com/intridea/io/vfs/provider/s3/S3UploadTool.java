package com.intridea.io.vfs.provider.s3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 * @version $Id$
 */
public class S3UploadTool {

    protected static final int REPORT_LEVEL_ALL = 3;
    private static final ByteFormatter byteFormatter = new ByteFormatter();
    private static final TimeFormatter timeFormatter = new TimeFormatter();

    public static void uploadSmallObject(AmazonS3 service, StorageObject targetObject) throws Exception {
        try {
            service.putObject(targetObject.getBucketName(), targetObject);
        } catch (AmazonServiceException e) {
            if ("EntityTooLarge".equals(e.getErrorCode())) {
                throw new IOException("Source object [" + targetObject.getDataInputFile() + "] too large for copying into S3");
            } else {
                throw e;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public static void uploadLargeObject(S3Service service, StorageObject targetObject
    ) throws Exception {
        final List<MultipartUpload> multipartUploadList = new ArrayList<MultipartUpload>();
        final List<MultipartUploadAndParts> uploadAndPartsList = new ArrayList<MultipartUploadAndParts>();

        // Event adaptor to handle multipart creation/completion events
        StorageServiceEventAdaptor multipartEventAdaptor = new S3ServiceEventAdaptor() {
            @Override
            public void event(MultipartStartsEvent event) {
                super.event(event);
                if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                    for (MultipartUpload upload: event.getStartedUploads()) {
                        multipartUploadList.add(upload);
                    }

                    displayProgressStatus("Starting large file uploads: ", event.getThreadWatcher());
                }
            }

            @Override
            public void event(MultipartCompletesEvent event) {
                super.event(event);
                if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                    displayProgressStatus("Completing large file uploads: ", event.getThreadWatcher());
                }
            }
        };

        // Start all multipart uploads
        (new ThreadedS3Service(service, multipartEventAdaptor)).multipartStartUploads(
                targetObject.getBucketName(), Arrays.asList(targetObject)
        );

        serviceEventAdaptor.throwErrorIfPresent();

        MultipartUtils multipartUtils = new MultipartUtils(S3FileObject.BIG_FILE_THRESHOLD);

        // Build upload and part lists from new multipart uploads
        for (MultipartUpload upload: multipartUploadList) {
            List<S3Object> partObjects = multipartUtils.splitFileIntoObjectsByMaxPartSize(
                    upload.getObjectKey(),
                    targetObject.getDataInputFile()
            );

            uploadAndPartsList.add(new MultipartUploadAndParts(upload, partObjects));
        }

        // Upload all parts for all multipart uploads
        (new ThreadedS3Service(service, serviceEventAdaptor)).multipartUploadParts(uploadAndPartsList);

        serviceEventAdaptor.throwErrorIfPresent();

        // Complete all multipart uploads
        (new ThreadedS3Service(service, multipartEventAdaptor)).multipartCompleteUploads(multipartUploadList);

        serviceEventAdaptor.throwErrorIfPresent();
    }

    private static void displayProgressStatus(String prefix, ThreadWatcher watcher) {
        String progressMessage = prefix + watcher.getCompletedThreads() + "/" + watcher.getThreadCount();

        // Show percentage of bytes transferred, if this info is available.
        if (watcher.isBytesTransferredInfoAvailable()) {
            String bytesTotalStr = byteFormatter.formatByteSize(watcher.getBytesTotal());
            long percentage = (int)
                (((double)watcher.getBytesTransferred() / watcher.getBytesTotal()) * 100);

            String detailsText = formatTransferDetails(watcher);

            progressMessage += " - " + percentage + "% of " + bytesTotalStr
                + (detailsText.length() > 0 ? " (" + detailsText + ")" : "");
        } else {
            long percentage = (int)
                (((double)watcher.getCompletedThreads() / watcher.getThreadCount()) * 100);

            progressMessage += " - " + percentage + "%";
        }
        printProgressLine(progressMessage);
    }

    private static void printProgressLine(String line) {
//        if (isQuiet || isNoProgress) {
//            return;
//        }

        String temporaryLine = "  " + line;
//        if (temporaryLine.length() > maxTemporaryStringLength) {
//            maxTemporaryStringLength = temporaryLine.length();
//        }
        String blanks = "";
//        for (int i = temporaryLine.length(); i < maxTemporaryStringLength; i++) {
//            blanks += " ";
//        }
        System.out.print(temporaryLine + blanks + "\r");
    }

    private static void printOutputLine(String line, int level) {
//        if ((isQuiet && level > REPORT_LEVEL_NONE) || reportLevel < level) {
//            return;
//        }

        String blanks = "";
//        for (int i = line.length(); i < maxTemporaryStringLength; i++) {
//            blanks += " ";
//        }
        System.out.println(line + blanks);
//        maxTemporaryStringLength = 0;
    }

    private static String formatTransferDetails(ThreadWatcher watcher) {
        String detailsText = "";
        long bytesPerSecond = watcher.getBytesPerSecond();
        detailsText = byteFormatter.formatByteSize(bytesPerSecond) + "/s";

        if (watcher.isTimeRemainingAvailable()) {
            if (detailsText.trim().length() > 0) {
                detailsText += " - ";
            }
            long secondsRemaining = watcher.getTimeRemaining();
            detailsText += "ETA: " + timeFormatter.formatTime(secondsRemaining, false);
        }
        return detailsText;
    }

    private static final StorageServiceEventAdaptor serviceEventAdaptor = new S3ServiceEventAdaptor() {
        private void displayIgnoredErrors(ServiceEvent event) {
            if (ServiceEvent.EVENT_IGNORED_ERRORS == event.getEventCode()) {
                Throwable[] throwables = event.getIgnoredErrors();
                for (int i = 0; i < throwables.length; i++) {
                    printOutputLine("Ignoring error: " + throwables[i].getMessage(), REPORT_LEVEL_ALL);
                }
            }
        }

        @Override
        public void event(CreateObjectsEvent event) {
            super.event(event);
            displayIgnoredErrors(event);
            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                displayProgressStatus("Upload: ", event.getThreadWatcher());
            }
        }

        @Override
        public void event(MultipartUploadsEvent event) {
            super.event(event);
            displayIgnoredErrors(event);
            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                displayProgressStatus("Upload large file parts: ", event.getThreadWatcher());
            }
        }

        @Override
        public void event(DownloadObjectsEvent event) {
            super.event(event);
            displayIgnoredErrors(event);
            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                displayProgressStatus("Download: ", event.getThreadWatcher());
            }
        }

        @Override
        public void event(GetObjectHeadsEvent event) {
            super.event(event);
            displayIgnoredErrors(event);
            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                displayProgressStatus("Retrieving object details from service: ", event.getThreadWatcher());
            }
        }

        @Override
        public void event(DeleteObjectsEvent event) {
            super.event(event);
            displayIgnoredErrors(event);
            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
                displayProgressStatus("Deleting objects in service: ", event.getThreadWatcher());
            }
        }
    };
}
