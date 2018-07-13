package com.ctfo.datacenter.cache.util;

public class ValidUtil {
    public static boolean validAddress(String address) {
        if ((address != null) && (!address.isEmpty())) {
            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\.(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\.(1\\d{2}|2[0-4]\\\td|25[0-5]|[1-9]\\d|\\d)\\:([1-5]?\\d{1,4}|6[1-5][1-5][1-3][1-5])$";

            return address.matches(regex);
        }
        return false;
    }

    public static void main(String[] args) {
        String s = "-12344";
        System.out.println(s.indexOf("-"));
    }
}
