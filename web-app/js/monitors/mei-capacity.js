(function($) {
    var aoColumns = [
        { "sTitle": "Cloud" },
        { "sTitle": "100"},
        { "sTitle": "200"},
        { "sTitle": "300"},
        { "sTitle": "400"},
        { "sTitle": "500"},
        { "sTitle": "600"},
        { "sTitle": "700"},
        { "sTitle": "800"},
        { "sTitle": "900"},
        { "sTitle": "1000"},
        { "sTitle": "1100"},
        { "sTitle": "1200"},
        { "sTitle": "1300"},
        { "sTitle": "1400"},
        { "sTitle": "1500"},
        { "sTitle": "1600"},
        { "sTitle": "1700"},
        { "sTitle": "1800"},
        { "sTitle": "1900"},
        { "sTitle": "2000"}
    ];

    var oTable;
    
    var cometd = $.cometd;


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
    var _connected = false;

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

    function _updateStats(data) {
        var dataObj = $.parseJSON(data);
        $("#last-update").text("Last Update: " + new Date());
        oTable.sAjaxSource = null;
        oTable.fnClearTable()
        oTable.fnAddData(dataObj.payload.rollup)
        oTable.fnDraw()
    }

    $(document).ready(function() {

        oTable = $("#stats-grid").dataTable({
            "aoColumns": aoColumns,
            "sAjaxSource" : "mei",
            "bPaginate": false,
            "bLengthChange": false,
            "bFilter": false,
            "bSort": false,
            "bInfo": false,
            "bAutoWidth": true,
            "bDeferRender": true,
            "bJQueryUI": true,
            "bProcessing": true,

            "aoColumnDefs": [
                {
                    "fnRender": function(oObj) {

                        var theData = oObj.aData[oObj.iDataColumn];
                        var num = parseInt(theData);

                        if(isNaN(num)) {
                            return theData;
                        }
                        if(num > 1200) {
                            return "<div style='color:red'>" + num + "</div>";
                        } else {
                            return "<div style='color:black'>" + num + "</div>";
                        }
                    },
                    "aTargets": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
                }
            ]
        });


        // TODO: parameterize this
        var location = document.location;
        var cometURL = location.protocol + "//" + location.host + "/monitor-server" + "/cometd";

        cometd.configure({
            url: cometURL,
            logLevel: 'warn'
        });

        cometd.addListener('/meta/handshake', _metaHandshake);
        cometd.addListener('/meta/connect', _metaConnect);

        cometd.handshake();

    });
    
})(jQuery);
