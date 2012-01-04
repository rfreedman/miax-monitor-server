package monitor.server;

import java.util.HashMap;
import java.util.Map;

public class StatStorageItem {
    String type;
    Integer cloudId;
    Integer mpId;
    Integer appId;
    long timestamp;
    Map<String, Integer> stats;
    Map<String, String> props;

    public StatStorageItem() {
        this.stats = new HashMap<String, Integer>();
        this.props = new HashMap<String, String>();
    }

    public StatStorageItem(String type, Integer cloudId, Integer mpId, Integer appId, long timestamp) {
        this();
        setType(type);
        setCloudId(cloudId);
        setMpId(mpId);
        setAppId(appId);
        setTimestamp(timestamp);
    }

    public StatStorageItem addStat(String key, Integer value) {
        stats.put(key, value);
        return this;
    }

    public StatStorageItem addProp(String key, String value) {
        props.put(key, value);
        return this;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getCloudId() {
        return cloudId;
    }

    public void setCloudId(Integer cloudId) {
        this.cloudId = cloudId;
    }

    public Integer getMpId() {
        return mpId;
    }

    public void setMpId(Integer mpId) {
        this.mpId = mpId;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Integer> getStats() {
        return stats;
    }

    public void setStats(Map<String, Integer> stats) {
        this.stats = stats;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(HashMap<String, String> props) {
        this.props = props;
    }
}
