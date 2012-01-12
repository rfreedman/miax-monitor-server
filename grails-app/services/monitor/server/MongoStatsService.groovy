package monitor.server

import com.gmongo.GMongo
import com.mongodb.BasicDBList

import com.mongodb.DB
import com.mongodb.DBCollection
import grails.converters.JSON
import org.springframework.beans.factory.InitializingBean

import org.cometd.bayeux.server.BayeuxServer
import org.cometd.bayeux.server.ServerSession
import org.cometd.bayeux.server.ServerMessage.Mutable

class MongoStatsService implements InitializingBean {

    static transactional = true
    GMongo mongo = new GMongo()
    DB db = mongo.getDB("miax-stats")

    def bayeux
    def bayeuxSession

    private HashMap<String, Runnable> publishers = new HashMap<String, Runnable>();

    void afterPropertiesSet() {

        // TODO: generate publishers based on configuration
        // TODO: have configuration of supported stats (publishers), and a known list of roll-up permutations, and generate publishers from configuration
        StatPublisher publisher = new StatPublisher("/rollups/mei-capacity-by-cloud", "MeiCapacityPublisher", meiCapacityRollup)
        publishers.put(publisher.channelName, publisher)

        publisher = new StatPublisher("/rollups/mei-latency-by-cloud", "MeiLatencyPublisher", meiLatencyRollup)
        publishers.put(publisher.channelName, publisher)

        bayeux.addExtension(new MetaListenerExtension());
        bayeuxSession = bayeux.newLocalSession()
        bayeuxSession.handshake()
    }

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

    public Map<Integer, Map<String, Integer>> rollUpAppTypeStatsByCloud(String appType, String statType) {

        Map<Integer, Map<String, Integer>> rollUp = new HashMap<Integer, Map<String, Integer>>();

        BasicDBList result = db.current_stats.group(
                [cloud: true], // keys to group by
                [type: appType, stat: statType], // conditions ('where' clause)
                getInitialValues(appType, statType),
                // reduce function
                """
                function(doc, out){
                    var stats = doc.stats;
                    for (var prop in stats) {
                        if(out[prop] === 'undefined') {
                            out[prop] = stats[prop];
                        } else {
                            out[prop] += stats[prop];
                        }
                    }
                }
                """
                // optional 'finalize' function
        )

        for (def r: result.iterator()) {
            def cloud = r.cloud.intValue();
            def stats = new HashMap<String, Integer>();
            for (def entry: r.entrySet()) {
                if (entry.key != "cloud") {
                    stats.put(entry.key, entry.value.intValue())
                }
            }

            rollUp.put(cloud, stats)
        }

        return rollUp
    }

    private Object getInitialValues(String appType, String statType) {
        DBCollection collection = db.getCollection("current_stats");
        def sampleStat = collection.findOne([type: appType, stat: statType]);
        def stats = sampleStat.stats;
        def retval = [:]
        for (stat in stats) {
            retval[stat.key] = 0
        }
        return retval
    }


    public void deleteAllStats() {
        db.dropDatabase()
        db = mongo.getDB("miax-stats")
    }


    def meiCapacityRollup = {
        try {
            Map<Integer, Map<String, Integer>> rollUps = rollUpAppTypeStatsByCloud("MEI", "capacity");

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
            Map<Integer, Map<String, Integer>> rollUps = rollUpAppTypeStatsByCloud("MEI", "latency");

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

    public static void main(String[] args) throws Exception {

        MongoStatsService service = new MongoStatsService()
        service.deleteAllStats();
        //def rollup = service.rollUpAppTypeStatsByCloud("MEI");
        //println rollup;
        /*
        service.ensureTestData();

        Thread.sleep(1000);

        /*
        long before = new Date().getTime();
        new MongoStatsService().ensureTestData();
        long after = new Date().getTime();
        def duration = after - before
        println "added data in ${duration} msec."


        for(int test = 0; test < 2; test++) {
            long before = new Date().getTime();
            def rollUp = service.rollUpAppTypeStatsByCloud("mei")
            long after = new Date().getTime();
            def duration = after - before
            println "rollup in ${duration} msec:"
            //println rollUp
            for(def entry : rollUp.entrySet()) {
                println "${entry.key}:${entry.value}"
            }

        }
        */
    }

    class StatPublisher implements Runnable {
        private String channelName;
        private String description;
        private Closure statClosure;
        private shouldRun = false;

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
                new Thread(this).start()
            }
        }

        public boolean isPublishing() {
            return shouldRun
        }

        public void stopPublishing() {
            shouldRun = false;
        }

        private def hasSubscribers = {
          def channel = bayeux.getChannel(channelName)
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
            long lastPublished = 0;
            long lastDuration = 0;
            shouldRun = true

            while (shouldRun) {
                try {
                    def age = new Date().getTime() - lastPublished
                    if (age >= 2000 - lastDuration) { // throttle publishing speed
                        def before = new Date().getTime()
                        publishRollup();
                        lastPublished = new Date().getTime();
                        def after = new Date().getTime()
                        def duration = after - before
                        lastDuration = duration
                        println "${description}: published in ${duration} msec."
                    } else {
                        Thread.sleep(100);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
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
