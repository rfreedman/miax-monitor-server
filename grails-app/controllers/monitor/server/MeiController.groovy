package monitor.server

import grails.converters.JSON

class MeiController {

    def mongoStatsService

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
                aaData: mongoStatsService.emptyMeiCapacityRollup()
        ]

        render data as JSON
    }

    def latencyData = {
        def data = [
                aaData: mongoStatsService.emptyMeiLatencyRollup()
        ]

        render data as JSON
    }
}
