/**
 * Created by Ahmad Alkilani on 5/1/2016.
 */
package object domain {
  case class Activity(
                       course_id: String,
                       user_id: String,
                       session_id: String,
                       activity_event: String,
                       time: String
                     )

  case class ActivityByProduct(product: String,
                               timestamp_hour: Long,
                               purchase_count: Long,
                               add_to_cart_count: Long,
                               page_view_count: Long)

  case class VisitorsByProduct(product: String, timestamp_hour: Long, unique_visitors: Long)
}
