package com.ctfo.datacenter.cache.config;

public class CTFODataBean {
    private String address;
    private String name;
    private String master_address;
    private String slave_address;
    private String cpu = "80";
    private String io = "80";
    private String mem = "80";
    private String[] area;

    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String[] getArea() {
        return this.area;
    }

    public void setArea(String[] area) {
        this.area = area;
    }
}
