package com.dellemc.sdp.demo.music;

import java.util.SplittableRandom;

/***
 * NOTE: this class is *not* thread-safe
 */
public class SongPlayer {
    private static SplittableRandom random = new SplittableRandom();

    private long playerId;
    private String partnerService;
    private SubscriptionLevel subscriptionLevel;
    private SongEvent nextEvent;
    private SongEvent lastEvent;

    public SongPlayer(long playerId) {
        this.playerId = playerId;
        this.subscriptionLevel = randomSubscriptionLevel();
        if (this.subscriptionLevel == SubscriptionLevel.PartnerMember)
            this.partnerService = "Prime";
    }

    SongEvent peekEvent() {
        return _nextEvent(false);
    }

    SongEvent nextEvent() {
        return _nextEvent(true);
    }

    private SongEvent _nextEvent(boolean consume) {
        if (nextEvent == null) {
            nextEvent = generateEvent();
        }

        SongEvent event = nextEvent;
        if (consume) {
            nextEvent = null;
        }
        return event;
    }

    // generate the next player event, including the time at which it occurs, following a reasonable behavior pattern
    private SongEvent generateEvent() {
        long now = System.currentTimeMillis();
        SongEvent event = new SongEvent();
        event.playerId = this.playerId;
        event.partnerService = this.partnerService;
        event.subscriptionLevel = this.subscriptionLevel;
        event.lastContext = new SongEvent.Context();
        event.nextContext = new SongEvent.Context();

        // if we just started, we won't have a lastEvent, so make one up that occurred in the past, to infer behavior
        if (lastEvent == null) {
            lastEvent = new SongEvent();
            lastEvent.songEventType = SongEventType.Next;
            lastEvent.nextContext = new SongEvent.Context();
            selectNewList(lastEvent);
            selectNewSong(lastEvent);
            lastEvent.timestamp = now - random.nextInt(30000); // some time in the last 30 seconds
        }

        event.lastContext.listType = lastEvent.nextContext.listType;
        event.lastContext.playlist = lastEvent.nextContext.playlist;
        event.lastContext.station = lastEvent.nextContext.station;
        event.lastContext.artist = lastEvent.nextContext.artist;
        event.lastContext.album = lastEvent.nextContext.album;
        event.lastContext.song = lastEvent.nextContext.song;

        // are we paused?  how long til we unpause?
        if (lastEvent.songEventType == SongEventType.Pause) {
            // 17% chance we pick a different song when resuming (unless we were playing a single song)
            if (random.nextInt(100) < 17 || event.lastContext.listType == SongEvent.ListType.SingleSong) {
                event.songEventType = SongEventType.Select;
                selectNewList(event);
                selectNewSong(event);
            } else {
                event.songEventType = SongEventType.Resume;
                copyListInfo(event.lastContext, event.nextContext);
                copySongInfo(event.lastContext, event.nextContext);
            }
            event.timestamp = lastEvent.timestamp + random.nextInt(1170000) + 30000; // between 30 seconds and 20 minutes

            // we are not currently paused - check if we need to pause (1% chance)
        } else if (random.nextInt(100) < 1) {
            event.songEventType = SongEventType.Pause;
            copyListInfo(event.lastContext, event.nextContext);
            copySongInfo(event.lastContext, event.nextContext);
            // pause should happen some time in the middle of the last song played
            event.timestamp = lastEvent.timestamp + random.nextInt(SongList.getLengthFor(event.lastContext.song) * 1000);

        } else { // not pausing or resuming

            if (likeThisSong(event.lastContext.listType)) { // if we like the current song
                if (event.lastContext.listType == SongEvent.ListType.SingleSong) {
                    // if we are playing a single song, they player will automatically pause at the end of it
                    event.songEventType = SongEventType.Pause;
                    copyListInfo(event.lastContext, event.nextContext);
                    copySongInfo(event.lastContext, event.nextContext);
                } else {
                    // wait for the next song in the list
                    event.songEventType = SongEventType.Next;
                    copyListInfo(event.lastContext, event.nextContext);
                    selectNewSong(event);
                }
                event.timestamp = lastEvent.timestamp + SongList.getLengthFor(event.lastContext.song) * 1000;
            } else if (likeCurrentList(event.lastContext.listType)) { // if we like the current list (album, station, playlist, etc.)
                // skip song, keep list
                event.songEventType = SongEventType.Skip;
                copyListInfo(event.lastContext, event.nextContext);
                selectNewSong(event);
                event.timestamp = lastEvent.timestamp + random.nextInt(3000) + 2000; // skip within 2-5 seconds
            } else {
                // select new list
                event.songEventType = SongEventType.Select;
                selectNewList(event);
                selectNewSong(event);
                event.timestamp = lastEvent.timestamp + random.nextInt(3000) + 2000; // skip within 2-5 seconds
            }
        }

        // make sure we don't send a late event on purpose
        if (event.timestamp < now) event.timestamp = now + 5;

        lastEvent = event;
        return event;
    }

    private SubscriptionLevel randomSubscriptionLevel() {
        // 37% are on free tier
        // 19% = paid members
        // 25% = members through partners
        // 12% = 30-day promo
        // 7% = 90-day promo
        int chance = random.nextInt(100);
        if (chance < 37) return SubscriptionLevel.FreeTier;
        if (chance < 56) return SubscriptionLevel.Member;
        if (chance < 81) return SubscriptionLevel.PartnerMember;
        if (chance < 93) return SubscriptionLevel.Promo30;
        return SubscriptionLevel.Promo90;
    }

    private boolean likeThisSong(SongEvent.ListType listType) {
        // if in a playlist, we are 82% likely to like the current song
        // if in a station, we are 23% likely to like the song
        // if in an album, we are 32% likely to like the song
        // if we picked the song, we like it
        int chance = random.nextInt(100);
        if (listType == SongEvent.ListType.Playlist) return chance < 82;
        if (listType == SongEvent.ListType.Station) return chance < 23;
        if (listType == SongEvent.ListType.Album) return chance < 32;
        return true;
    }

    // if this is called, it means we do not like the current song
    private boolean likeCurrentList(SongEvent.ListType listType) {
        // if in a playlist, we are 94% likely to like the list
        // if in a station, we are 87% likely to like the list
        // if in an album, we are 83% likely
        int chance = random.nextInt(100);
        if (listType == SongEvent.ListType.Playlist) return chance < 94;
        if (listType == SongEvent.ListType.Station) return chance < 87;
        if (listType == SongEvent.ListType.Album) return chance < 83;
        return false;
    }

    // copies list info (listType, playlist, station) from A to B
    private void copyListInfo(SongEvent.Context contextA, SongEvent.Context contextB) {
        contextB.listType = contextA.listType;
        contextB.playlist = contextA.playlist;
        contextB.station = contextA.station;
    }

    // copies song info (artist, album, song) from A to B
    private void copySongInfo(SongEvent.Context contextA, SongEvent.Context contextB) {
        contextB.artist = contextA.artist;
        contextB.album = contextA.album;
        contextB.song = contextA.song;
    }

    private void selectNewList(SongEvent event) {
        // weighted (50% playlist, 35% station, 13% one song, 2% album)
        int chance = random.nextInt(100);
        if (chance < 50) {
            event.nextContext.listType = SongEvent.ListType.Playlist;
            event.nextContext.playlist = "Fake Playlist";
        } else if (chance < 85) {
            event.nextContext.listType = SongEvent.ListType.Station;
            event.nextContext.station = "Fake Station";
        } else if (chance < 98) {
            event.nextContext.listType = SongEvent.ListType.SingleSong;
        } else {
            event.nextContext.listType = SongEvent.ListType.Album;
        }
    }

    private void selectNewSong(SongEvent event) {
        // pull a random song from the song list
        event.nextContext.song = SongList.getRandomSong();

        // get the artist
        event.nextContext.artist = SongList.getArtistFor(event.nextContext.song);

        // TODO: get real Albums
        event.nextContext.album = "Fake Album";
    }
}
