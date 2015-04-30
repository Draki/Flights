package com.stratio.model.spark.streaming

import com.stratio.model.FlightTicket
import org.apache.spark.streaming.dstream.DStream


class FlightTicketFunctions(self: DStream[FlightTicket]){

  def userAvgByWindow(): Unit = {
    self.context.StreamingContextState.
  self.map(value => (1f, 1f)).reduceBy
  }
}

trait FlightTicketDsl {

  implicit def flightFunctions(tickets: DStream[FlightTicket]):
    FlightTicketFunctions = new FlightTicketFunctions(tickets)
}

object FlightTicketDsl extends FlightTicketDsl
