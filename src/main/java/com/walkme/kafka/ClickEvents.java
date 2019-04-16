package com.walkme.kafka;

import java.util.ArrayList;

public class ClickEvents {

    public ArrayList<FullEvent> fullevents = new ArrayList<FullEvent>();

    public void addEvent(FullEvent fullEvent) {
        fullevents.add(fullEvent);
    }
}