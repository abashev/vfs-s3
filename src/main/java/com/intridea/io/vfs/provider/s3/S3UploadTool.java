package com.intridea.io.vfs.provider.s3;

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author Alexey Abashev
 * @author Moritz Siuts
 */
public class S3UploadTool {

    protected static final int REPORT_LEVEL_ALL = 3;

    public static void uploadSmallObject(AmazonS3 service, S3Object targetObject, File file) throws Exception {
        try {
            PutObjectRequest putReq = new PutObjectRequest(targetObject.getBucketName(), targetObject.getKey(), file);
            putReq.setMetadata(targetObject.getObjectMetadata());
            service.putObject(putReq);
        } catch (AmazonServiceException e) {
            if ("EntityTooLarge".equals(e.getErrorCode())) {
                throw new IOException("Source object [" + file.getName() + "] too large for copying into S3");
            } else {
                throw e;
            }
        } catch (Exception e) {
            throw e;
        }
    }

//    public static void uploadLargeObject(AmazonS3 service, S3Object targetObject) throws Exception {
//        final List<MultipartUpload> multipartUploadList = new ArrayList<MultipartUpload>();
//        final List<MultipartUploadAndParts> uploadAndPartsList = new ArrayList<MultipartUploadAndParts>();
//
//        // Event adaptor to handle multipart creation/completion events
//        StorageServiceEventAdaptor multipartEventAdaptor = new S3ServiceEventAdaptor() {
//            @Override
//            public void event(MultipartStartsEvent event) {
//                super.event(event);
//                if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                    for (MultipartUpload upload: event.getStartedUploads()) {
//                        multipartUploadList.add(upload);
//                    }
//
//                    displayProgressStatus("Starting large file uploads: ", event.getThreadWatcher());
//                }
//            }
//
//            @Override
//            public void event(MultipartCompletesEvent event) {
//                super.event(event);
//                if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                    displayProgressStatus("Completing large file uploads: ", event.getThreadWatcher());
//                }
//            }
//        };
//
//        // Start all multipart uploads
//        (new ThreadedS3Service(service, multipartEventAdaptor)).multipartStartUploads(
//                targetObject.getBucketName(), Arrays.asList(targetObject)
//        );
//
//        serviceEventAdaptor.throwErrorIfPresent();
//
//        MultipartUtils multipartUtils = new MultipartUtils(S3FileObject.BIG_FILE_THRESHOLD);
//
//        // Build upload and part lists from new multipart uploads
//        for (MultipartUpload upload: multipartUploadList) {
//            List<S3Object> partObjects = multipartUtils.splitFileIntoObjectsByMaxPartSize(
//                    upload.getObjectKey(),
//                    targetObject.getDataInputFile()
//            );
//
//            uploadAndPartsList.add(new MultipartUploadAndParts(upload, partObjects));
//        }
//
//        // Upload all parts for all multipart uploads
//        (new ThreadedS3Service(service, serviceEventAdaptor)).multipartUploadParts(uploadAndPartsList);
//
//        serviceEventAdaptor.throwErrorIfPresent();
//
//        // Complete all multipart uploads
//        (new ThreadedS3Service(service, multipartEventAdaptor)).multipartCompleteUploads(multipartUploadList);
//
//        serviceEventAdaptor.throwErrorIfPresent();
//    }
//
//
//
//    private static final StorageServiceEventAdaptor serviceEventAdaptor = new S3ServiceEventAdaptor() {
//        private void displayIgnoredErrors(ServiceEvent event) {
//            if (ServiceEvent.EVENT_IGNORED_ERRORS == event.getEventCode()) {
//                Throwable[] throwables = event.getIgnoredErrors();
//                for (int i = 0; i < throwables.length; i++) {
//                    printOutputLine("Ignoring error: " + throwables[i].getMessage(), REPORT_LEVEL_ALL);
//                }
//            }
//        }
//
//        @Override
//        public void event(CreateObjectsEvent event) {
//            super.event(event);
//            displayIgnoredErrors(event);
//            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                displayProgressStatus("Upload: ", event.getThreadWatcher());
//            }
//        }
//
//        @Override
//        public void event(MultipartUploadsEvent event) {
//            super.event(event);
//            displayIgnoredErrors(event);
//            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                displayProgressStatus("Upload large file parts: ", event.getThreadWatcher());
//            }
//        }
//
//        @Override
//        public void event(DownloadObjectsEvent event) {
//            super.event(event);
//            displayIgnoredErrors(event);
//            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                displayProgressStatus("Download: ", event.getThreadWatcher());
//            }
//        }
//
//        @Override
//        public void event(GetObjectHeadsEvent event) {
//            super.event(event);
//            displayIgnoredErrors(event);
//            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                displayProgressStatus("Retrieving object details from service: ", event.getThreadWatcher());
//            }
//        }
//
//        @Override
//        public void event(DeleteObjectsEvent event) {
//            super.event(event);
//            displayIgnoredErrors(event);
//            if (ServiceEvent.EVENT_IN_PROGRESS == event.getEventCode()) {
//                displayProgressStatus("Deleting objects in service: ", event.getThreadWatcher());
//            }
//        }
//    };
}
