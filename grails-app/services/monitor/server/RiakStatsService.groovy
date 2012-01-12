package monitor.server

import com.basho.riak.client.IRiakObject
import com.basho.riak.client.query.indexes.BinIndex
import com.basho.riak.client.raw.RawClient
import com.basho.riak.client.raw.RiakResponse
import com.basho.riak.client.raw.pbc.PBClientConfig
import com.basho.riak.client.raw.pbc.PBClusterClient
import com.basho.riak.client.raw.pbc.PBClusterConfig
import com.basho.riak.client.raw.query.indexes.BinValueQuery
import com.basho.riak.client.raw.query.indexes.IndexQuery
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import org.springframework.beans.factory.InitializingBean
import grails.converters.JSON

class RiakStatsService implements InitializingBean {

    static transactional = true
    private static final String CURRENT_STATS_BUCKET = "current_stats";
    private static final String USER_META_STATS_KEY = "xxx-user-meta-stats-xxx";
    private static final String USER_META_STATS_KEY_DELIMITER = "|";


    RawClient rawClient;

    def bayeux
    def bayeuxSession

    void afterPropertiesSet() {
        initializeRiakClient()
        bayeuxSession = bayeux.newLocalSession()
        bayeuxSession.handshake()
        new Thread(new MeiPublisher()).start()
    }

    private void initializeRiakClient() throws Exception {
        String host = "127.0.0.1";
        int port = 8081;
        long connectionTimeout = 500;

        int maxConnections = 0;

        PBClientConfig.Builder builder = new PBClientConfig.Builder();
        PBClusterConfig clusterConfig = new PBClusterConfig(maxConnections);

        clusterConfig.addClient(
                builder.withHost(host).withPort(8081).withConnectionTimeoutMillis(connectionTimeout).build());


        clusterConfig.addClient(
                builder.withHost(host).withPort(8082).withConnectionTimeoutMillis(connectionTimeout).build());

        clusterConfig.addClient(
                builder.withHost(host).withPort(8083).withConnectionTimeoutMillis(connectionTimeout).build());

        clusterConfig.addClient(
                builder.withHost(host).withPort(8084).withConnectionTimeoutMillis(connectionTimeout).build());

        clusterConfig.addClient(
                builder.withHost(host).withPort(8085).withConnectionTimeoutMillis(connectionTimeout).build());

        clusterConfig.addClient(
                builder.withHost(host).withPort(8086).withConnectionTimeoutMillis(connectionTimeout).build());

        rawClient = new PBClusterClient(clusterConfig);
        rawClient.generateAndSetClientId();
    }

    /**
     * Gets all of the keys for the current statistics packets matching the parameters.
     *
     * @param type The service type (e.g. "MEI") - Mandatory - may not be null
     * @param cloudId - The Cloud identifier - may be null
     * @param mpId - The Market Participant identifier - may be null
     * @param appId - The application instance id - may be null
     *                <p/>
     *                A null value for cloudId, mpId, or appId means "don't care", so for example, specifying just 'type'
     *                will get all keys the type, regardless of cloud, firm, or app instance,
     *                while specifying 'type' and 'mpId' will get keys for the specified type for the specified firm.
     */
    Collection<String> getCurrentItemKeys(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        StringBuilder indexName = new StringBuilder("type");
        StringBuilder indexValue = new StringBuilder(type);

        if (mpId != null) {
            indexName.append("-").append("mpid");
            indexValue.append("-").append(mpId);
        }

        if (cloudId != null) {
            indexName.append("-").append("cloudid");
            indexValue.append("-").append(cloudId);
        }

        if (appId != null) {
            indexName.append("-").append("appid");
            indexValue.append("-").append(appId);
        }

        IndexQuery query = new BinValueQuery(BinIndex.named(indexName.toString()), CURRENT_STATS_BUCKET, indexValue.toString());
        List<String> keys = rawClient.fetchIndex(query);

        return keys;
    }

    public Map<Integer, Map<String, Integer>> rollUpAppTypeStatsByCloud(String appType) throws Exception {

        Map<Integer, Map<String, Integer>> cloudRollups = new TreeMap<Integer, Map<String, Integer>>(
                new Comparator<Integer>() {
                    public int compare(Integer integer, Integer integer1) {
                        return integer.compareTo(integer1);
                    }
                }
        );

        long before = new Date().getTime();
        Collection<StatStorageItem> items = getCurrentItemsMultiThreaded("MEI", null, null, null);
        long after = new Date().getTime();
        long duration = after - before;
        System.out.println("retrieved " + items.size() + " items in " + duration + " msec.");

        before = new Date().getTime();

        for (StatStorageItem item: items) {
            // find the rollup for this item's cloud
            int cloudId = item.getCloudId();
            Map<String, Integer> rollUp = cloudRollups.get(cloudId);
            if (rollUp == null) {
                rollUp = new HashMap<String, Integer>();
                cloudRollups.put(cloudId, rollUp);
            }

            for (Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
                if (rollUp.containsKey(entry.getKey())) {
                    rollUp.put(entry.getKey(), rollUp.get(entry.getKey()) + entry.getValue());
                } else {
                    rollUp.put(entry.getKey(), entry.getValue());
                }
            }

        }

        after = new Date().getTime();
        duration = after - before;
        System.out.println("rollup took " + duration + " msec.");

        return cloudRollups;
    }


    public Collection<StatStorageItem> getCurrentItemsMultiThreaded(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        Collection<StatStorageItem> items = new ArrayList<StatStorageItem>();
        String[] keys = getCurrentItemKeys(type, cloudId, mpId, appId).toArray(new String[0]);

        int CLUSTER_SIZE = 6;
        ExecutorService pool = Executors.newFixedThreadPool(CLUSTER_SIZE);
        Set<Future<Collection<StatStorageItem>>> futures = new HashSet<Future<Collection<StatStorageItem>>>();

        for (String key: keys) {
            Callable<Collection<StatStorageItem>> callable = new StatsRetriever(Arrays.asList(key));
            Future<Collection<StatStorageItem>> future = pool.submit(callable);
            futures.add(future);
        }


        for (Future<Collection<StatStorageItem>> future: futures) {
            items.addAll(future.get());
        }

        return items;
    }

    //// ========== multi-threading stuff ======================
    public class StatsRetriever implements Callable<Collection<StatStorageItem>> {
        private Collection<String> keys;
        private Collection<StatStorageItem> items = new ArrayList<StatStorageItem>();

        public StatsRetriever(Collection<String> itemKeys) {
            this.keys = itemKeys;
        }

        public Collection<StatStorageItem> call() {
            try {
                for (String key: keys) {
                    StatStorageItem item = getCurrentItemByKey(key);
                    items.add(item);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return items;
        }

        public StatStorageItem getCurrentItemByKey(String key) throws Exception {
            RiakResponse fetched = rawClient.fetch(CURRENT_STATS_BUCKET, key);
            IRiakObject result = fetched.getRiakObjects()[0];
            StatStorageItem item = null;

            if (result != null) {
                String type = (String) result.getBinIndex("type").toArray()[0];
                Integer cloudId = (Integer) result.getIntIndex("cloudid").toArray()[0];
                Integer mpId = (Integer) result.getIntIndex("mpid").toArray()[0];
                Integer appId = (Integer) result.getIntIndex("appid").toArray()[0];
                Long value = new Long(result.getValueAsString());
                item = new StatStorageItem(type, cloudId, mpId, appId, value);
                item.setStats(getStats(result));
            }

            return item;
        }

    }

    private Map<String, Integer> getStats(IRiakObject item) {
        String keyList = item.getUsermeta(USER_META_STATS_KEY);
        StringTokenizer tokenizer = new StringTokenizer(keyList, USER_META_STATS_KEY_DELIMITER);
        Map<String, Integer> stats = new HashMap<String, Integer>();
        while (tokenizer.hasMoreTokens()) {
            String key = tokenizer.nextToken();
            String value = item.getUsermeta(key);
            stats.put(key, new Integer(value));
        }
        return stats;
    }

    def emptyMeiRollup = {
        def stats = [];

        def stat = ["Waiting...", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""];
        stats.push stat;
        return stats
    }

    def meiRollup = {
        try{
            Map<Integer, Map<String, Integer>> rollUps = rollUpAppTypeStatsByCloud("MEI");

            def stats = [];

            for (Integer key: rollUps.keySet()) {
                //System.out.println(key + ": " + rollUps.get(key));

                def aStat = [key];
                Map<String, Integer> rollup = rollUps.get(key)
                for (Map.Entry<String, Integer> entry: rollup.entrySet()) {
                    aStat.push entry.value
                }
                stats.push aStat
            }

            return stats
        } catch(Exception ex) {
            ex.printStackTrace()
            return emptyMeiRollup()
        }
    }

    class MeiPublisher implements Runnable {
        
        void publishRollup() {
            
                def rollUp = meiRollup()
                long before = new Date().getTime()

                def jsonData = ["payload":["rollup":rollUp]] as JSON
                def jsonString = jsonData.toString()
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud').publish(jsonString)

                /*
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-1').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-2').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-3').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-4').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-5').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-6').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-7').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-8').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-9').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-10').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-11').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-12').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-13').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-14').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-15').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-16').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-17').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-18').publish(['payload':['rollup':rollUp]] as JSON)
                bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud-19').publish(['payload':['rollup':rollUp]] as JSON)
                */

                long after = new Date().getTime()
                long duration = after - before
                println "** published to 20 channels in ${duration} msec."
        }

        void run() {
            long lastPublished = 0;
            while(true) {
                try {
                    def age = new Date().getTime() - lastPublished
                    if( age >= 1000) {
                        def before = new Date().getTime()
                        publishRollup();
                        lastPublished = new Date().getTime();
                        def after = new Date().getTime()
                        def duration = after - before
                        println "published rollup in ${duration} msec."
                    } else {
                        Thread.sleep(100);
                    }
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
