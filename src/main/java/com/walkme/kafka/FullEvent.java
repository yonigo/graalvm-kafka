package com.walkme.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties
public class FullEvent {
    public String type;
    public String routingKey;
    public MessageHeader headers;
    public String body;
    public Boolean isErrorClick;
}