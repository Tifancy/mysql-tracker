package com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.mariadb;

import com.github.hackerwin7.mysql.tracker.mysql.dbsync.LogBuffer;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.FormatDescriptionLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.IgnorableLogEvent;
import com.github.hackerwin7.mysql.tracker.mysql.dbsync.event.LogHeader;

/**
 * mariadb的ANNOTATE_ROWS_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午2:20:35
 * @since 1.0.17
 */
public class AnnotateRowsEvent extends IgnorableLogEvent {

    private String rowsQuery;

    public AnnotateRowsEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);

        final int commonHeaderLen = descriptionEvent.getCommonHeaderLen();
        final int postHeaderLen = descriptionEvent.getPostHeaderLen()[header.getType() - 1];

        int offset = commonHeaderLen + postHeaderLen;
        int len = buffer.limit() - offset;
        rowsQuery = buffer.getFullString(offset, len, LogBuffer.ISO_8859_1);
    }

    public String getRowsQuery() {
        return rowsQuery;
    }

}
