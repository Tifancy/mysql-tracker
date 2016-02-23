package com.github.hackerwin7.mysql.tracker.tracker.parser;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.*;
import com.github.hackerwin7.mysql.tracker.protocol.protobuf.SimpleEntry;
import com.github.hackerwin7.mysql.tracker.tracker.common.AviaterRegexFilter;
import com.github.hackerwin7.mysql.tracker.tracker.common.TableMetaCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2016/02/23
 * Time: 5:24 PM
 * Desc: simply convert event
 * Tips:
 */
public class SimpleLogEventConvert {
    public static final String          ISO_8859_1          = "ISO-8859-1";
    public static final String          UTF_8               = "UTF-8";
    public static final int             TINYINT_MAX_VALUE   = 256;
    public static final int             SMALLINT_MAX_VALUE  = 65536;
    public static final int             MEDIUMINT_MAX_VALUE = 16777216;
    public static final long            INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE    = new BigInteger("18446744073709551616");
    public static final int             version             = 1;
    public static final String          BEGIN               = "BEGIN";
    public static final String          COMMIT              = "COMMIT";
    public static final Logger logger              = LoggerFactory.getLogger(LogEventConvert.class);

    private volatile AviaterRegexFilter nameFilter;                                                          // 运行时引用可能会有变化，比如规则发生变化时
    private TableMetaCache tableMetaCache;

    private String                      binlogFileName      = "mysql-bin.000001";
    private Charset charset             = Charset.defaultCharset();
    private boolean                     filterQueryDcl      = false;
    private boolean                     filterQueryDml      = false;
    private boolean                     filterQueryDdl      = false;

    //additional info
    private static long                 batchId             = 0;
    private static long                 inId                = 0;
    private static String               ip                  = "";
    public static Map<String, String> filterMap = new HashMap<String, String>();


    public void setBatchId(long id) {
        batchId = id;
    }

    public void setInId(long id) {
        inId = id;
    }

    public void setIp(String address) {
        ip = address;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    /**
     * parse event to entry
     * @param logEvent
     * @return simple entry
     * @throws Exception
     */
    public SimpleEntry.Entry parse(LogEvent logEvent) throws Exception {
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                logger.info("EVENT : rotate");
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                logger.info("EVENT : query");
                return parseQueryEvent((QueryLogEvent) logEvent);
            case LogEvent.XID_EVENT:
                logger.info("EVENT : xid");
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                logger.info("EVENT : table_map");
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                logger.info("EVENT : write_rows");
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
                logger.info("EVENT : update_rows");
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                logger.info("EVENT : delete_rows");
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    /**
     * parse event to transaction begin or end entry
     * @param event
     * @return entry
     */
    private SimpleEntry.Entry parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, SimpleEntry.EntryType.TRANSACTIONBEGIN, null);
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, SimpleEntry.EntryType.TRANSACTIONEND, null);
        } else {
            // ddl
            // no op
            return null;
        }
    }

    /**
     * parse to transaction end
     * @param event
     * @return entry
     */
    private SimpleEntry.Entry parseXidEvent(XidLogEvent event) {
        SimpleEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
        return createEntry(header, SimpleEntry.EntryType.TRANSACTIONEND, null);
    }

    private SimpleEntry.Entry parseRowsEvent(RowsLogEvent event) {
        
    }

    /**
     * create simple entry header
     * @param binlogFile
     * @param logHeader
     * @param dbName
     * @param tbName
     * @param eventType
     * @return header
     */
    private SimpleEntry.Header createHeader(String binlogFile, LogHeader logHeader, String dbName, String tbName, SimpleEntry.EventType eventType) {
        SimpleEntry.Header.Builder headerBuilder = SimpleEntry.Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setTs(logHeader.getWhen() * 1000L);
        headerBuilder.setEventLen(logHeader.getEventLen());
        if(eventType != null)
            headerBuilder.setEventType(eventType);
        if(StringUtils.isBlank(dbName))
            headerBuilder.setDatabaseName(dbName);
        if(StringUtils.isBlank(tbName))
            headerBuilder.setTableName(tbName);
        return headerBuilder.build();
    }

    /**
     * build the entry
     * @param header
     * @param entryType
     * @param rowChange
     * @return entry
     */
    private SimpleEntry.Entry createEntry(SimpleEntry.Header header, SimpleEntry.EntryType entryType, SimpleEntry.RowChange rowChange) {
        SimpleEntry.Entry.Builder entryBuilder = SimpleEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        if(rowChange != null)
            entryBuilder.setRowChange(rowChange);
        return entryBuilder.build();
    }
}
