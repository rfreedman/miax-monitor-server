package monitor.server

import grails.converters.JSON

class MeiController {

    def rdbStatsService

    def index = {
    }

    def bulk_quote_capacity_by_cloud = {
    }

    def bulk_quote_capacity_graph = {

    }

    def bulk_quote_latency_by_cloud = {
    }

    def data = {

        def data = [
                aaData: rdbStatsService.emptyMeiCapacityRollup()
        ]

        render data as JSON
    }

    def latencyData = {
        def data = [
                aaData: rdbStatsService.emptyMeiLatencyRollup()
        ]

        render data as JSON
    }
}
