(function($) {

    var cometd = null;
    var _connected = false;

    var plot;
    var data = [[new Date().getTime(), 0]];
    var totalPoints = 60;
    var updateInterval = 2000;
    var options = {
        series: { shadowSize: 0 }, // drawing is faster without shadows
        yaxis: { min: 0, max: 5000 },
        //xaxis: { show: false }
        xaxis: { mode: "time", timeformat: "%h:%M:%S:%p"}
    };
    

    function _connectionEstablished() {
        $('#body').append('<div>CometD Connection Established</div>');
    }

    function _connectionBroken() {
        $('#body').append('<div>CometD Connection Broken</div>');
    }

    function _connectionClosed() {
        $('#body').append('<div>CometD Connection Closed</div>');
    }

    // Function that manages the connection status with the Bayeux server
    function _metaConnect(message) {
        if (cometd.isDisconnected()) {
            _connected = false;
            _connectionClosed();
            return;
        }

        var wasConnected = _connected;
        _connected = message.successful === true;
        if (!wasConnected && _connected) {
            _connectionEstablished();
        }
        else if (wasConnected && !_connected) {
            _connectionBroken();
        }
    }

    // Function invoked when first contacting the server and
    // when the server has lost the state of this client
    function _metaHandshake(handshake) {
        if (handshake.successful === true) {
            //if(console) { console.log("handshake successful") }
            cometd.batch(function() {
                cometd.subscribe('/rollups/mei-capacity-by-cloud', function(message) {
                    //if(console) { console.log("got message: " + message.data) }
                    // TODO: update the table
                    _updateStats(message.data)
                });

            });
        } else {
            // if(console){ console.log("handshake unsuccessful") }
        }
    }

    function _updateStats(stat) {

        var dataObj = $.parseJSON(stat);
        var value = dataObj.payload.rollup[0][1];
        var now = new Date();

        if(data.length > totalPoints) {
            data = data.slice(1);
        }
        data.push([now.getTime(), value]);
        plot.setData([data]);
        plot.setupGrid();
        plot.draw();

        $("#last-update").text("Last Update: " + now + ", last value = " + value);

    }
 
    // ==========================================================================

    function getRandomData() {

        if (data.length > 0)
            data = data.slice(1);

        // do a random walk
        while (data.length < totalPoints) {
            var prev = data.length > 0 ? data[data.length - 1] : 50;
            var y = prev + Math.random() * 10 - 5;
            if (y < 0)
                y = 0;
            if (y > 100) {
                y = 100;
            }
            y = y * 10;
            data.push(y);
        }

        // zip the generated y values with the x values
        var res = [];
        for (var i = 0; i < data.length; ++i) {
           res.push([i, data[i]])
           //res.push([new Date().getTime(), data[i]])
        }

        return res;
        

    }

    function update() {

        if(data.length > totalPoints) {
            data = data.slice(1);
        }
        data.push([new Date().getTime(),  Math.random() * 100]);
        plot.setData([data]);
        
        // since the axes don't change, we don't need to call plot.setupGrid()
        plot.setupGrid();
        plot.draw();

        setTimeout(update, updateInterval);
    }

    // ===========================================================================


    $(document).ready(function() {

        plot = $.plot($("#placeholder"), [data], options);
        //update();


        // TODO: parameterize this
        var location = document.location;
        var cometURL = location.protocol + "//" + location.host + "/monitor-server" + "/cometd";

        cometd = $.cometd;
        
        cometd.configure({
            url: cometURL,
            logLevel: 'warn'
        });


        cometd.addListener('/meta/handshake', _metaHandshake);
        cometd.addListener('/meta/connect', _metaConnect);

        cometd.handshake();

    });

})(jQuery);
