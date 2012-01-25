package monitor.server

import grails.converters.JSON
import groovy.sql.Sql
import org.cometd.bayeux.server.BayeuxServer
import org.cometd.bayeux.server.ServerMessage.Mutable
import org.cometd.bayeux.server.ServerSession
import org.springframework.beans.factory.InitializingBean
import groovy.sql.GroovyRowResult
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

class RdbStatsService implements InitializingBean {

    def dataSource;

    def bayeux
    def bayeuxSession

    private HashMap<String, Runnable> publishers = new HashMap<String, Runnable>();
    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(50);

    void afterPropertiesSet() {
        // TODO: generate publishers based on configuration
        // TODO: have configuration of supported stats (publishers), and a known list of roll-up permutations, and generate publishers from configuration
        StatPublisher publisher = new StatPublisher("/rollups/mei-capacity-by-cloud", "MeiCapacityPublisher", meiCapacityRollup)
        publishers.put(publisher.channelName, publisher)

        publisher = new StatPublisher("/rollups/mei-latency-by-cloud", "MeiLatencyPublisher", meiLatencyRollup)
        publishers.put(publisher.channelName, publisher)


        int extraQueryCount = 1000;
        for (int i = 0; i < extraQueryCount; i++) {
            StatPublisher pub = new StatPublisher("/rollups/mei-capacity-by-cloud-" + (i + 1), "MeiCapacityPublisher" + (i + 1), meiCapacityRollup)
            publishers.put(pub.channelName, pub)
            println "configured publisher for channel: ${pub.channelName}"
        }


        bayeux.addExtension(new MetaListenerExtension());
        bayeuxSession = bayeux.newLocalSession()
        bayeuxSession.handshake()
    }

    static transactional = true

    // TODO: can we do away with the 'empty' rollup closures, or at least generate the emoties dynamically, in way similar to what getInitialValues() does ?
    def emptyMeiCapacityRollup = {
        def stats = [];

        def stat = ["Waiting...", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""];
        stats.push stat;
        return stats
    }

    def emptyMeiLatencyRollup = {
        def stats = [];

        def stat = ["Waiting...", "", "", "", "", ""];
        stats.push stat;
        return stats
    }


    static final Map<String, List<String>> statColumnMap = new HashMap<String, List<String>>();
    static {
        statColumnMap.put("STAT_BULK_QUOTE_CAPACITY_MSEC", [
              "cap_100",
              "cap_200",
              "cap_300",
              "cap_400",
              "cap_500",
              "cap_600",
              "cap_700",
              "cap_800",
              "cap_900",
              "cap_1000",
              "cap_1100",
              "cap_1200",
              "cap_1300",
              "cap_1400",
              "cap_1500",
              "cap_1600",
              "cap_1700",
              "cap_1800",
              "cap_1900",
              "cap_2000"
        ])


        statColumnMap.put("STAT_BULK_QUOTE_REQ_LATENCY_NSEC", [
            "lat_0_199",
            "lat_200_399",
            "lat_400_599",
            "lat_600_799",
            "lat_800_1799",
            "lat_1800_INF"
        ])
    }

    // TODO - this will be custom for each individual statName, driven by configuration
    // for now, we just sum all columns
    public Map<Integer, Map<String, Integer>> rollUpAppTypeStatsByCloud(String statName) {

        Map<Integer, Map<String, Integer>> rollUp = new HashMap<Integer, Map<String, Integer>>();
        List<String> statColumns = statColumnMap.get(statName)

        def db = new Sql(dataSource) // Create a new instance of groovy.sql.Sql with the DB of the Grails app

        StringBuilder buf = new StringBuilder("select cloud ")
        for(String col in statColumns) {
            buf.append(", sum(${col}) as ${col} ")
        }
        buf.append(" from ${statName} group by cloud")

        List<GroovyRowResult> results = db.rows(buf.toString()) // Perform the query
        for(GroovyRowResult result in results) {
            def cloud = result.get("cloud")
            def stats = new HashMap<String, Integer>();
            stats.putAll(result);
            stats.remove("cloud")
            rollUp.put(cloud, stats)
        }

        return rollUp
    }

    def meiCapacityRollup = {
        try {
            Map<Integer, Map<String, Integer>> rollUps = rollUpAppTypeStatsByCloud("STAT_BULK_QUOTE_CAPACITY_MSEC");

            def stats = [];

            for (Integer key: rollUps.keySet()) {
                def aStat = [key];
                Map<String, Integer> rollup = rollUps.get(key)
                for (Map.Entry<String, Integer> entry: rollup.entrySet()) {
                    aStat.push entry.value
                }
                stats.push aStat
            }

            return stats
        } catch (Exception ex) {
            ex.printStackTrace()
            return emptyMeiCapacityRollup()
        }
    }

    def meiLatencyRollup = {
        try {
            Map<Integer, Map<String, Integer>> rollUps = rollUpAppTypeStatsByCloud("STAT_BULK_QUOTE_REQ_LATENCY_NSEC");

            def stats = [];

            for (Integer key: rollUps.keySet()) {
                def aStat = [key];
                Map<String, Integer> rollup = rollUps.get(key)
                for (Map.Entry<String, Integer> entry: rollup.entrySet()) {
                    aStat.push entry.value
                }
                stats.push aStat
            }

            return stats
        } catch (Exception ex) {
            ex.printStackTrace()
            return emptyMeiLatencyRollup()
        }
    }


    class StatPublisher implements Runnable {
        private String channelName;
        private String description;
        private Closure statClosure;
        private shouldRun = false;

        private def future = null;

        public StatPublisher(String channelName, String description, Closure statClosure) {
            this.channelName = channelName
            this.description = description
            this.statClosure = statClosure
        }

        public def getChannelName = {
            channelName
        }

        public def ensurePublishing = {
            if (!publishing) {
                println "${description} - got subscription, and not publishing yet, so starting to publish"
                //new Thread(this).start()
               future =  scheduler.scheduleAtFixedRate(this, 100, 2000, TimeUnit.MILLISECONDS)
            }
        }

        public boolean isPublishing() {
            return shouldRun
        }

        public void stopPublishing() {
            shouldRun = false;
            if(future != null) {
                future.cancel(false);
                future = null;
            }
        }

        private def hasSubscribers = {

            def channel = bayeux.getChannel(channelName)
            if(channel == null) {
                println "channel ${channelName} doesn't seem to exist"
                return false
            }

            channel.getSubscribers().size() > 0
        }

        private def getChannel = {
            bayeuxSession.getChannel(channelName)
        }

        void publishRollup() {
            if (hasSubscribers()) {
                def rollUp = statClosure()
                def jsonData = ["payload": ["rollup": rollUp]] as JSON
                def jsonString = jsonData.toString()
                getChannel().publish(jsonString)
            } else {
                println "${description}: no subscribers, so stopping"
                stopPublishing()
            }
        }

        void run() {
            //long lastPublished = 0;
           // long lastDuration = 0;
            shouldRun = true

           // while (shouldRun) {
                try {
                    //def age = new Date().getTime() - lastPublished
                    //if (age >= 2000 - lastDuration) { // throttle publishing speed
                        def before = new Date().getTime()
                        publishRollup();
                        //lastPublished = new Date().getTime();
                        def after = new Date().getTime()
                        def duration = after - before
                        //lastDuration = duration
                        println "${description}: published in ${duration} msec."
                   // } else {
                   //     Thread.sleep(100);
                  //  }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
           // }
        }

    }

    class MetaListenerExtension implements BayeuxServer.Extension {

        boolean rcvMeta(ServerSession serverSession, Mutable meta) {
            if (meta.channel.toString().endsWith("/subscribe")) {

                def publisher = publishers.get(meta.subscription)

                if (publisher) {
                    publisher.ensurePublishing()
                } else {
                    println "didn't find publisher for subscribe to ${meta.subscription}: publishers = ${publishers}"
                }
            }
            return true
        }

        boolean rcv(ServerSession serverSession, Mutable mutable) {
            return true
        }

        boolean send(ServerSession serverSession, ServerSession serverSession1, Mutable mutable) {
            return true
        }

        boolean sendMeta(ServerSession serverSession, Mutable mutable) {
            return true
        }
    }
}
