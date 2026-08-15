package com.paras.eventplatform.dlq;

import com.paras.eventplatform.event.EventRecord;

import java.util.List;

public interface DeadLetterQueue {
    void add(EventRecord event, int attempts, String error);

    List<DeadLetter> findAll();

    void clear();
}
