package monitor.server

import grails.converters.JSON

class MeiController {

    def statsService

    def index = {

        def data = [
                /*
                aaData: [
                        ["1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                        ["2", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
                ] */

                aaData: statsService.meiRollup()

        ]

        render data as JSON
    }
}
