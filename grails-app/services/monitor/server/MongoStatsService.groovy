package monitor.server

import com.gmongo.GMongo
import com.mongodb.BasicDBList

import com.mongodb.DB
import com.mongodb.DBCollection
import grails.converters.JSON
import org.springframework.beans.factory.InitializingBean
import org.cometd.bayeux.Channel
import org.cometd.bayeux.server.BayeuxServer
import org.cometd.bayeux.server.ServerSession
import org.cometd.bayeux.server.ServerMessage.Mutable
import java.util.concurrent.atomic.AtomicInteger

class MongoStatsService implements InitializingBean {

    static transactional = true
    GMongo mongo = new GMongo()
    DB db = mongo.getDB("miax-stats")

    def bayeux
    def bayeuxSession

    private HashMap<String, Runnable> publishers = new HashMap<String, Runnable>();
    ///private HashMap<String, Set<String>> subscriptions = new HashMap<String, Set<String>>();




    void afterPropertiesSet() {

        publishers.put("/rollups/mei-capacity-by-cloud",  new MeiCapacityPublisher())
        publishers.put("/rollups/mei-latency-by-cloud",   new MeiLatencyPublisher())

        bayeux.addExtension(new MetaListenerExtension());
        bayeuxSession = bayeux.newLocalSession()
        bayeuxSession.handshake()
    }

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
        def sampleStat = collection.findOne([type:appType, stat:statType]);
        def stats = sampleStat.stats;
        def retval = [:]
        for(stat in stats) {
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
                //System.out.println(key + ": " + rollUps.get(key));

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

    class MeiCapacityPublisher implements Runnable {

        private shouldRun = false;
        private AtomicInteger subscriberCount = new AtomicInteger(0);

        public def addSubscriber =  {
            def currentCount = subscriberCount.incrementAndGet().intValue()
            println("MeiCapacityPbublisher - new subscriber, count now ${currentCount}")

            if(!publishing) {
                println "MeiCapacityPublisher - got subscription, and not publishing yet, so starting to publish"
                new Thread(this).start()
            }
        }

        public def removeSubscriber = {
           def currentCount = subscriberCount.decrementAndGet().intValue()
           println "MeiCapacityPublisher - subscriber removed, count now ${currentCount}"
           if(currentCount < 1) {
               subscriberCount.set(0)
               shouldRun = false
               println "MeiCapacityPublisher - last subscriber unsubscribed, so stopping"
           }
        }

        public boolean isPublishing() {
            return shouldRun
        }

        public void stopPublishing() {
            shouldRun = false;
        }

        void publishRollup() {

            def rollUp = meiCapacityRollup()
            //long before = new Date().getTime()

            def jsonData = ["payload": ["rollup": rollUp]] as JSON
            def jsonString = jsonData.toString()
            bayeuxSession.getChannel('/rollups/mei-capacity-by-cloud').publish(jsonString)

            //long after = new Date().getTime()
            //long duration = after - before
            //println "** published mei capacity rollup in ${duration} msec."
        }

        void run() {
            long lastPublished = 0;
            long lastDuration = 0;
            shouldRun = true

            while (shouldRun) {
                try {
                    def age = new Date().getTime() - lastPublished
                    if (age >= 2000 - lastDuration) {
                        def before = new Date().getTime()
                        publishRollup();
                        lastPublished = new Date().getTime();
                        def after = new Date().getTime()
                        def duration = after - before
                        lastDuration = duration
                        println "published mei capacity rollup in ${duration} msec."
                    } else {
                        Thread.sleep(100);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


     class MeiLatencyPublisher implements Runnable {
        private shouldRun = false;

        private AtomicInteger subscriberCount = new AtomicInteger(0);

        public def addSubscriber =  {
            def currentCount = subscriberCount.incrementAndGet()
            println("MeiLatencyPublisher - new subscriber, count now ${currentCount}")

            if(!publishing) {
                println "MeiLatencyPublisher - got subscription, and not publishing yet, so starting to publish"
                new Thread(this).start()
            }
        }

        public def removeSubscriber = {
          def currentCount = subscriberCount.decrementAndGet()
          println("MeiLatencyPublisher - removed subscriber, count now ${currentCount}")
           if(currentCount < 1) {
               subscriberCount.set(0)
               shouldRun = false
               println "MeiLatencyPublisher -  last subscriber unsubscribed, so stopping"
           }
        }

        public boolean isPublishing() {
            return shouldRun
        }
         
        public void stopPublishing() {
            shouldRun = false;
        }

        void publishRollup() {
            def rollUp = meiLatencyRollup()
            def jsonData = ["payload": ["rollup": rollUp]] as JSON
            def jsonString = jsonData.toString()
            bayeuxSession.getChannel('/rollups/mei-latency-by-cloud').publish(jsonString)
        }

        void run() {
            long lastPublished = 0;
            long lastDuration = 0;

            shouldRun = true;

            while (shouldRun) {
                try {
                    def age = new Date().getTime() - lastPublished
                    if (age >= 2000 - lastDuration) {
                        def before = new Date().getTime()
                        publishRollup();
                        lastPublished = new Date().getTime();
                        def after = new Date().getTime()
                        def duration = after - before
                        lastDuration = duration
                        println "published mei latency rollup in ${duration} msec."
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

            for(;;) {
                if(meta.channel.toString().endsWith("/subscribe")) {

                   def publisher = publishers.get(meta.subscription)

                   if(publisher) {
                       publisher.addSubscriber()
                   } else {
                       println "didn't find publisher for subscribe to ${meta.subscription}"
                   }

                   break;
                }

                if(meta.channel.toString().endsWith("/unsubscribe")) {
                   def publisher = publishers.get(meta.subscription)
                   if(publisher) {
                       publisher.removeSubscriber()
                   } else {
                       println "didn't find publisher for unsubscribe from ${meta.subscription}"
                   }

                    break;
                }

                break;
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
