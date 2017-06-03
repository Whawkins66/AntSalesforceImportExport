import com.sforce.async.*;
import com.sforce.soap.partner.Connector;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.apache.tools.ant.Task;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by spatnaik on 31/05/2017.
 */
public class SFBulkImport extends Task {

    private String username;
    private String password;
    private String serverURL;
    private String operation;
    private String sObjectType;
    private String filename;
    private String externalId;
    private String apiVersion = "39.0";
    private Boolean debug = false;

    public void execute() {
        try {
            BulkConnection connection = BulkAPIHelper.getConnection(username, password, serverURL, apiVersion, debug);
            JobInfo job = BulkAPIHelper.createJob(sObjectType, connection, operation, externalId);
            List<BatchInfo> batchInfos = BulkAPIHelper.createBatchesFromCSVFile(connection, job, filename);
            BulkAPIHelper.closeJob(connection, job);
            BulkAPIHelper.awaitCompletion(connection, job, batchInfos, debug);

            if (debug)
                BulkAPIHelper.checkResults(connection, job, batchInfos);

        } catch (ConnectionException | AsyncApiException | IOException e) {
            log(String.format("Error executing job %s", e.getMessage()));
        }
    }

    public void setsObjectType(String sObjectType) {
        this.sObjectType = sObjectType;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setServerURL(String serverURL) {
        this.serverURL = serverURL;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }
}
