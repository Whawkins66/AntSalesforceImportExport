import com.sforce.async.*;
import com.sforce.soap.partner.Connector;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.*;
import java.util.*;

/**
 * Created by spatnaik on 02/06/2017.
 */
public class BulkAPIHelper {
    public static BulkConnection getConnection(
            String username, String password,
            String serverURL, String apiVersion,
            Boolean debug)
            throws ConnectionException, AsyncApiException {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(username);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(serverURL + "/services/Soap/u/" + apiVersion);

        Connector.newConnection(partnerConfig);
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());

        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(!debug);
        config.setTraceMessage(debug);
        return new BulkConnection(config);
    }

    /**
     * Create a new job using the Bulk API.
     *
     * @param sobjectType
     * The object type being loaded, such as "Account"
     * @param connection
     * BulkConnection used to create the new job.
     * @return The JobInfo for the new job.
     * @throws AsyncApiException
     */
    public static JobInfo createJob(String sobjectType, BulkConnection connection, String operation, String externalId)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(Enum.valueOf(OperationEnum.class, operation));
        job.setContentType(ContentType.CSV);

        if (operation.compareTo("upsert") == 0)
            job.setExternalIdFieldName(externalId);

        job = connection.createJob(job);
        return job;
    }

    public static List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, String csvFileName)
            throws IOException {
        BufferedReader rdr = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFileName)));

        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("SFBulkImport", ".csv");

        List<BatchInfo> batchInfos = new ArrayList<>();

        // Split the CSV file into multiple batches
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000; // 10 million bytes per batch
            int maxRowsPerBatch = 10000; // 10 thousand rows per batch
            int currentBytes = 0;
            int currentLines = 0;
            String nextLine;
            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
                // Create a new batch when our batch size limit is reached
                if (currentBytes + bytes.length > maxBytesPerBatch
                        || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }

                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }

                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }

            // Finished processing all rows
            // Create a final batch for any remaining data
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } catch (AsyncApiException e) {
            e.printStackTrace();
        } finally {
            tmpFile.delete();
        }

        return batchInfos;
    }

    /**
     * Create a batch by uploading the contents of the file.
     * This closes the output stream.
     *
     * @param tmpOut
     * The output stream used to write the CSV data for a single batch.
     * @param tmpFile
     * The file associated with the above stream.
     * @param batchInfos
     * The batch info for the newly created batch is added to this list.
     * @param connection
     * The BulkConnection used to create the new batch.
     * @param jobInfo
     * The JobInfo associated with the new batch.
     */
    public static void createBatch(FileOutputStream tmpOut, File tmpFile,
                             List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
            throws IOException, AsyncApiException {

        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);
        try {
            BatchInfo batchInfo =
                    connection.createBatchFromStream(jobInfo, tmpInputStream);
            batchInfos.add(batchInfo);
        } finally {
            tmpInputStream.close();
        }
    }

    public static void closeJob(BulkConnection connection, JobInfo job)
            throws AsyncApiException {

        JobInfo closeJob = new JobInfo();
        closeJob.setId(job.getId());
        closeJob.setState(JobStateEnum.Closed);
        connection.updateJob(closeJob);
    }

    /**
     * Wait for a job to complete by polling the Bulk API.
     *
     * @param connection
     * BulkConnection used to check results.
     * @param job
     * The job awaiting completion.
     * @param batchInfoList
     * List of batches for this job.
     * @throws AsyncApiException
     */
    public static void awaitCompletion(
            BulkConnection connection, JobInfo job,
            List<BatchInfo> batchInfoList, Boolean debug)
            throws AsyncApiException {

        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}

            if (debug)
                System.out.println("Awaiting results..." + incomplete.size());

            sleepTime = 10000L;
            BatchInfo[] statusList =
                    connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                        || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        if (debug)
                            System.out.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }

    /**
     * Gets the results of the operation and checks for errors.
     */
    public static void checkResults(BulkConnection connection, JobInfo job,
                              List<BatchInfo> batchInfoList)
            throws AsyncApiException, IOException {

        PrintWriter writer = null;
        writer = new PrintWriter("result_" + job.getId() + ".csv", "UTF-8");

        // Write Header
        writer.println("Status,Id,Error");

        // batchInfoList was populated when batches were created and submitted
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr =
                    new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();
            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }

                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");


                writer.println(String.format("%s,%s,%b", success ? "Success" : "Error", id, error));
            }
        }


        writer.close();
    }
}
