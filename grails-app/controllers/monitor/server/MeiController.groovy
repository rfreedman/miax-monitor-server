package monitor.server

import grails.converters.JSON

class MeiController {

    def statsService

    def index = {

        def data = [
                aaData: statsService.emptyMeiRollup()
        ]

        render data as JSON
    }
}
