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

$(document).ready(function() {

    $("#stats-grid").dataTable({
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
        "bProcessing": true
    });
});