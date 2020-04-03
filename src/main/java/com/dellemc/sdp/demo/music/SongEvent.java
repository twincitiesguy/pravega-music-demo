package com.dellemc.sdp.demo.music;

// the assumption is that this is a music service like Spotify or Pandora
public class SongEvent {
    public long timestamp;
    public long playerId;
    public SubscriptionLevel subscriptionLevel;
    public String partnerService;
    public SongEventType songEventType;
    public Context lastContext;
    public Context nextContext;

    enum ListType {
        Album, Playlist, Station, SingleSong;
    }

    public static class Context {
        public ListType listType;
        public String playlist;
        public String station;
        public String artist;
        public String album;
        public String song;
    }
}
