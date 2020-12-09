# cce-stream-processors - place holder repo

## Use-case 

The GPE-Ingress component receives price-update events from GPE. These events are at per-country-per-model level.

After retrieving the price document from GPE (per-country-per-model), the next steps are to compute the virtual price list and send it to price-service, where more heavy computations happen.

The trouble is, virtual price list is at country level. When multiple models' (for the same country) price document gets updated in a short burst, price-service is burdened with several heavy re-computations unnecessarily.

If there is a way to aggregate all price-update events by country over a time window, then the gpe-ingress component could be engineered to send ONE virtual price list that contains updated price values received over several events (for different models).

Also a desired behavior: Price updates events for several countries may be (and should be) processed in parallel.

It was examined if there was a possibility to accumulate a set of records in Kafka stream before triggering a process with batched input of the record-set.

Streams API - with TimeWindow aggregation fits this use case. 

+++ NOTE ++++ Kafka streams and processor APIs are available ONLY in Java as of this writing.

However, a blocker was encountered with this approach - it works ONLY if there is steady stream of events due to dependence on "stream-time":
https://stackoverflow.com/questions/54222594/kafka-stream-suppress-session-windowed-aggregation
https://stackoverflow.com/questions/60822669/kafka-sessionwindow-with-suppress-only-sends-final-event-when-there-is-a-steady

If a burst of price-update events happen for the Sweden, followed by no activity on any other country for extended times, then aggregate operation for Sweden would NOT be triggered in a timely way. Markets shall complain about stale price data being presented by CCE service.

This example implements a workaround streams-transformation topology suggested in the links above. This solution will add one more microservice to the ecosystem and becomes viable if there are additional stream processing requirements in CCE. Spring is also an option for such a microservice.
