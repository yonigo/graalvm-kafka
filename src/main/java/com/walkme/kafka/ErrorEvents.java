package com.walkme.kafka;

import java.util.ArrayList;

public class ErrorEvents {

    public ArrayList<FullEvent> fullevents = new ArrayList<FullEvent>();

    public void addErrorEvent(FullEvent fullEvent) {
        fullevents.add(fullEvent);
    }
}