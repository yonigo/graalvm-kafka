package com.walkme.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class MessageHeader {
    public String sessionId;
    public String windowId;
    public Long sessionStartTime;
    public String action;
    public String parentWindowId;
    public String endUserInstanceId;
    public String uid;
    public String sequence;
    public Long sequenceIndex;
    public String version;
    public Boolean isCompressed;
    public String apiKey;
}