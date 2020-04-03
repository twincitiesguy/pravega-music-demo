# Music Plays demo for Pravega Streaming Storage

This demo is intended to be similar to the Kafka Music demo.  There is a generator app that generates random song play events, a processing app that tracks windowed statistics (like song counts, artist counts, player counts, etc.), and a small REST service that provides access to these statistics.

## Running the generator

To run the generator, simply build the shadowJar (`./gradlew shadowJar`) and follow the usage instructions:

```
usage: SongPlayGenerator [-c <controller-uri>] [-d] [-h] [-k] [-p
       <num-players>] [-s <pravega-stream>] [-v] [-x <pravega-scope>]
 -c,--controller <controller-uri>   Service endpoint of the Pravega
                                    controller
 -d,--debug                         Debug logging
 -h,--help                          Print this help text
 -k,--use-keycloak                  This enables Keycloak authentication
                                    for use with Streaming Data Platform.
                                    You must have a valid keycloak.json
                                    file in your home directory
 -p,--players <num-players>         Number of players/users to simulate.
                                    Each player will simulate live user
                                    behavior. Default is 100 (~8 events
                                    per second)
 -s,--stream <pravega-stream>       The Pravega stream name
 -v,--verbose                       Verbose logging
 -x,--scope <pravega-scope>         The Pravega scope
```

(You can generate these instructions by running the jar with the `-h` or `--help` option) 

### Sample JSON format
```$json
{
  "timestamp": 1585895510886, // <- this is the event time 
  "playerId": 123, // <- this is the routing key
  "subscriptionLevel": "Promo30",
  "partnerService": null,
  "songEventType": "Skip",
  "lastContext": {
    "listType": "Station",
    "playlist": null,
    "station": "Fake Station",
    "artist": "Tag Team",
    "album": "Fake Album",
    "song": "Whoomp! (There It Is)"
  },
  "nextContext": {
    "listType": "Station",
    "playlist": null,
    "station": "Fake Station",
    "artist": "Bee Gees",
    "album": "Fake Album",
    "song": "Night Fever"
  }
}
```
