package com.ctfo.datacenter.cache.config;

import java.util.List;

public class CTFOConfigBean {
    private String address;
    private String master_address;
    private String slave_address;
    private String cpu = "80";
    private String io = "80";
    private String mem = "80";
    private int moveThread = 5;
    private int deleteThread = 5;
    private List<CTFODataBean> ctfoDataBean;

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getMaster_address() {
        return this.master_address;
    }

    public void setMaster_address(String master_address) {
        this.master_address = master_address;
    }

    public String getSlave_address() {
        return this.slave_address;
    }

    public void setSlave_address(String slave_address) {
        this.slave_address = slave_address;
    }

    public String getCpu() {
        return this.cpu;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
    }

    public String getIo() {
        return this.io;
    }

    public void setIo(String io) {
        this.io = io;
    }

    public String getMem() {
        return this.mem;
    }

    public void setMem(String mem) {
        this.mem = mem;
    }

    public int getMoveThread() {
        return this.moveThread;
    }

    public void setMoveThread(int moveThread) {
        this.moveThread = moveThread;
    }

    public int getDeleteThread() {
        return this.deleteThread;
    }

    public void setDeleteThread(int deleteThread) {
        this.deleteThread = deleteThread;
    }

    public List<CTFODataBean> getCtfoDataBean() {
        return this.ctfoDataBean;
    }

    public void setCtfoDataBean(List<CTFODataBean> ctfoDataBean) {
        this.ctfoDataBean = ctfoDataBean;
    }
}
