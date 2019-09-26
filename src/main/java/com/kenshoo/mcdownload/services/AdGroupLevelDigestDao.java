package com.kenshoo.mcdownload.services;

import com.kenshoo.mcdownload.ConnectionPool;
import com.kenshoo.mcdownload.data.model.AdGroupLevelId;
import com.kenshoo.mcdownload.data.model.ChangeDetectorRequest;
import com.kenshoo.mcdownload.data.model.ChannelType;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class AdGroupLevelDigestDao {

    private static final Logger logger = LoggerFactory.getLogger(AdGroupLevelDigestDao.class);

    private static final String TABLE_FIELDS = " (job_id,bulk_id,client_name,channel_type, channel_account_id,campaign_id,adgroup_id,entity_id,digest);";

    private String adGroupLevelDigestTable = "neg_adgroup_keyword_digest";

    private String adGroupLevelDigestSideTable = "neg_adgroup_keyword_digest_side";

    public void loadAccountRecordsToSideTableFromFile(List<ChangeDetectorRequest<AdGroupLevelId>> entities,
                                                      long accountId, String jobId, int bulkId, String clientName,
                                                      ChannelType channelType) {

        File file = new File(jobId + bulkId + ".csv");
        try {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
                entities.forEach(Unchecked.consumer(entity -> writer.write(changeDetectorRequestToCSV(accountId, jobId, bulkId, clientName, channelType, entity))));
            }
            loadFileToTable(file, adGroupLevelDigestSideTable, jobId, bulkId, entities.size());
        } catch (IOException e) {
            logger.error("Could not save account records to file. JobId: " + jobId + " , bulkId: " + bulkId, e);
            throw new RuntimeException("Could not save account records to file", e);
        } finally {
            if (!file.delete()) {
                logger.warn("File wasn't deleted: {}", file.getName());
            }
        }
    }

    private void loadFileToTable(File input, String tableName, String jobId, int bulkId, int expectedChunkSize) {
        Connection connection = ConnectionPool.get();
        try {
            Statement query = connection.createStatement();

            long actualLoaded = query.executeLargeUpdate("LOAD DATA LOCAL INFILE '" + input.getPath() + "' \n" +
                    "INTO TABLE " + tableName + "\n" +
                    "FIELDS TERMINATED BY '\\t'" + TABLE_FIELDS);

            if (expectedChunkSize != actualLoaded) {
                logger.info("load jobId:{} , bulkId {} , loadCount {} , expectedChunkSize:{}", jobId, bulkId, actualLoaded, expectedChunkSize);
            }
            query.close();
        } catch (Exception e) {
            logger.error("Could not load InputStream into DB table: " + tableName, e);
            throw new RuntimeException("Could not load InputStream into DB table: " + tableName, e);
        } finally {
            Unchecked.runnable(connection::close).run();
        }
    }

    public Set<AdGroupLevelId> getChangedDigestsForBulk(long accountId, String jobId, int bulkId, String clientName) {

        Connection connection = ConnectionPool.get();

        try {
            Statement query = connection.createStatement();
            String QUERY_TEXT = "SELECT " + adGroupLevelDigestSideTable + ".campaign_id, " + adGroupLevelDigestSideTable + ".adgroup_id, " + adGroupLevelDigestSideTable + ".entity_id " +
                    "FROM " + adGroupLevelDigestSideTable + " " +
                    "LEFT JOIN " + adGroupLevelDigestTable + " " +
                    "ON " + adGroupLevelDigestSideTable + ".client_name = " + adGroupLevelDigestTable + ".client_name " +
                    "AND " + adGroupLevelDigestSideTable + ".channel_type = " + adGroupLevelDigestTable + ".channel_type " +
                    "AND " + adGroupLevelDigestSideTable + ".channel_account_id = " + adGroupLevelDigestTable + ".channel_account_id " +
                    "AND " + adGroupLevelDigestSideTable + ".campaign_id = " + adGroupLevelDigestTable + ".campaign_id " +
                    "AND " + adGroupLevelDigestSideTable + ".adgroup_id = " + adGroupLevelDigestTable + ".adgroup_id " +
                    "AND " + adGroupLevelDigestSideTable + ".entity_id = " + adGroupLevelDigestTable + ".entity_id " +
                    "AND " + adGroupLevelDigestTable + ".channel_account_id = '" + accountId + "' " +
                    "AND " + adGroupLevelDigestTable + ".client_name = '" + clientName + "' " +
                    "WHERE " + adGroupLevelDigestSideTable + ".channel_account_id = '" + accountId + "' " +
                    "AND " + adGroupLevelDigestSideTable + ".job_id = '" + jobId + "' " +
                    "AND " + adGroupLevelDigestSideTable + ".bulk_id = '" + bulkId + "' " +
                    "AND " + adGroupLevelDigestSideTable + ".client_name = '" + clientName + "' " +
                    "AND (" + adGroupLevelDigestTable + ".digest IS NULL OR  " + adGroupLevelDigestTable + ".digest != " + adGroupLevelDigestSideTable + ".digest) ";



            ResultSet rs = query.executeQuery(
                    QUERY_TEXT
            );

            Set<AdGroupLevelId> results = new HashSet<>();
            while (rs.next()) {
                results.add(toAdGroupLevelId(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
            rs.close();
            query.close();
            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            Unchecked.runnable(connection::close).run();
        }

    }

    public List<AdGroupLevelId> getEntitiesToDelete(long accountId, String
            jobId, List<String> campaignIds, String clientName) {

        Connection connection = ConnectionPool.get();

        try {

            Statement query = connection.createStatement();
            String QUERY_TEXT = "SELECT " + adGroupLevelDigestTable + ".campaign_id, " + adGroupLevelDigestTable + ".adgroup_id, " + adGroupLevelDigestTable + ".entity_id " +
                    "FROM " + adGroupLevelDigestTable + " " +
                    "LEFT JOIN " + adGroupLevelDigestSideTable + " " +
                    "ON " + adGroupLevelDigestSideTable + ".client_name = " + adGroupLevelDigestTable + ".client_name " +
                    "AND " + adGroupLevelDigestSideTable + ".channel_type = " + adGroupLevelDigestTable + ".channel_type " +
                    "AND " + adGroupLevelDigestSideTable + ".channel_account_id = " + adGroupLevelDigestTable + ".channel_account_id " +
                    "AND " + adGroupLevelDigestSideTable + ".campaign_id = " + adGroupLevelDigestTable + ".campaign_id " +
                    "AND " + adGroupLevelDigestSideTable + ".adgroup_id = " + adGroupLevelDigestTable + ".adgroup_id " +
                    "AND " + adGroupLevelDigestSideTable + ".entity_id = " + adGroupLevelDigestTable + ".entity_id " +
                    "AND " + adGroupLevelDigestTable + ".channel_account_id = '" + accountId + "' " +
                    "AND " + adGroupLevelDigestTable + ".client_name = '" + clientName + "' " +
                    "WHERE " + adGroupLevelDigestSideTable + ".channel_account_id = '" + accountId + "' " +
                    "AND " + adGroupLevelDigestSideTable + ".job_id = '" + jobId + "' " +
                    "AND " + adGroupLevelDigestSideTable + ".entity_id is NULL ";

            ResultSet rs = query.executeQuery(QUERY_TEXT);

            List<AdGroupLevelId> results = new ArrayList<>();
            while (rs.next()) {
                results.add(toAdGroupLevelId(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
            rs.close();
            query.close();
            return results;

        } catch (SQLException e) {
            Unchecked.runnable(connection::close).run();
            throw new RuntimeException(e);
        } finally {
            Unchecked.runnable(connection::close).run();
        }
    }

    public int countSideTable(String jobId) {
        int fetched = 0;
        Connection connection = ConnectionPool.get();
        try {
            Statement query = connection.createStatement();
            ResultSet rs = query.executeQuery("SELECT COUNT (1) FROM " + adGroupLevelDigestSideTable + " WHERE job_id='" + jobId + "'");
            rs.next();
            fetched = rs.getInt(1);
            rs.close();
            query.close();
        } catch (Exception e) {
            logger.error("exception when reading from side table, jobid: " + jobId, e);
        } finally {
            Unchecked.runnable(connection::close).run();
        }
        return fetched;
    }

    public int deleteAccountRecordsFromSideTable(String jobId) {
        int fetched = 0;
        Connection connection = ConnectionPool.get();
        try {
            Statement query = connection.createStatement();
            fetched = query.executeUpdate("DELETE FROM " + adGroupLevelDigestSideTable + " WHERE job_id='" + jobId + "'");
            query.close();
        } catch (Exception e) {
            logger.error("exception when reading from side table, jobid: " + jobId, e);
        } finally {
            Unchecked.runnable(connection::close).run();
        }
        return fetched;
    }

    private AdGroupLevelId toAdGroupLevelId(String value1, String value2, String value3) {
        return new AdGroupLevelId.Builder()
                .setCampaignID(Long.parseLong(value1))
                .setAdGroupId(Long.parseLong(value2))
                .setEntityId(Long.parseLong(value3)).build();
    }

    private String changeDetectorRequestToCSV(long accountId, String jobId, int bulkId, String clientName,
                                              ChannelType channelType, ChangeDetectorRequest<AdGroupLevelId> entity) {
        return jobId +
                "\t" + bulkId +
                "\t" + clientName +
                "\t" + channelType.ordinal() +
                "\t" + accountId +
                "\t" + entity.getId().getCampId() +
                "\t" + entity.getId().getAdgroupId() +
                "\t" + entity.getId().getEntityId() +
                "\t" + Base64.encode(entity.getDigest()) + "\n";
    }
}