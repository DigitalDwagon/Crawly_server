package dev.digitaldragon;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class URLInfo {

    //info from client
    public String url = null;
    public ArrayList<String> discovered = null;
    public String userAgent = null;
    public Long responseCode = null;
    public String crawlerUsername = null;

    //info gathered
    public String crawlerIp = null;
    public boolean crawled = false;
    public LocalDateTime crawlTime = null;

    public static void setCrawled(boolean newStatus) {

    }

}
