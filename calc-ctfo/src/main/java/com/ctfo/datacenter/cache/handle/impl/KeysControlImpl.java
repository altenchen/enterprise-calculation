package com.ctfo.datacenter.cache.handle.impl;

import com.ctfo.datacenter.cache.conn.ConfigControl;
import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.base.KeysControl;

import java.util.HashSet;
import java.util.Set;

public class KeysControlImpl
    implements KeysControl {
    private String key;
    private ConfigControl configControl;

    public KeysControlImpl(String key, ConfigControl configControl) {
        this.key = key;
        this.configControl = configControl;
    }

    @Override
    public Set<String> getkey(long count)
        throws DataCenterException {
        Set<String> set = new HashSet();
        for (int i = 0; i < count; i++) {
            String temp = this.configControl.popLList(this.key);
            if (temp == null) {
                break;
            }
            set.add(temp);
        }
        return set;
    }

    @Override
    public String getkey()
        throws DataCenterException {
        return this.configControl.popLList(this.key);
    }
}
