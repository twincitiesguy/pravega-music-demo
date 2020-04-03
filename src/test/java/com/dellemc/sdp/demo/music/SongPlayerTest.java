package com.dellemc.sdp.demo.music;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

public class SongPlayerTest {
    private static final Logger log = LoggerFactory.getLogger(SongPlayerTest.class);

    private static ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSongPlayer() throws Exception {
        SongPlayer songPlayer = new SongPlayer(123);
        // generate 10000 events and verify them
        long lastEventTime = 0;
        for (int i = 0; i < 10000; i++) {
            SongEvent event = songPlayer.nextEvent();
            verifyEvent(event, lastEventTime);
            lastEventTime = event.timestamp;
        }
    }

    private void verifyEvent(SongEvent event, long lastEventTime) throws Exception {
        log.info("event: {}", mapper.writeValueAsString(event));

        assertTrue(event.timestamp > System.currentTimeMillis());

        assertEquals(123, event.playerId);

        if (event.subscriptionLevel == SubscriptionLevel.PartnerMember) {
            assertNotNull(event.partnerService);
        } else {
            assertNull(event.partnerService);
        }

        verifyContext(event.lastContext);
        verifyContext(event.nextContext);

        if (event.songEventType == SongEventType.Select) {
            // this could happen 2-5 seconds after an event if we don't like the song, or could happen up to 20 minutes later if part of resuming
            if (lastEventTime > 0)
                assertTrue(event.timestamp - lastEventTime >= 2000);
        } else if (event.songEventType == SongEventType.Next) {
            // there's no "next" song when playing a single song
            assertNotEquals(SongEvent.ListType.SingleSong, event.lastContext.listType);
            // list remains the same
            assertEquals(event.lastContext.listType, event.nextContext.listType);
            assertEquals(event.lastContext.playlist, event.nextContext.playlist);
            assertEquals(event.lastContext.station, event.nextContext.station);
            // this should happen after the last song plays all the way through
            if (lastEventTime > 0)
                assertEquals(SongList.getLengthFor(event.lastContext.song).longValue() * 1000, event.timestamp - lastEventTime);
        } else if (event.songEventType == SongEventType.Skip) {
            // there's no "skip" when playing a single song
            assertNotEquals(SongEvent.ListType.SingleSong, event.lastContext.listType);
            // list remains the same
            assertEquals(event.lastContext.listType, event.nextContext.listType);
            assertEquals(event.lastContext.playlist, event.nextContext.playlist);
            assertEquals(event.lastContext.station, event.nextContext.station);
            // should happen between 2-5 seconds after starting the last song
            if (lastEventTime > 0)
                assertTrue(event.timestamp - lastEventTime < 5000 && event.timestamp - lastEventTime >= 2000);
        } else if (event.songEventType == SongEventType.Pause) {
            assertEquals(event.lastContext.listType, event.nextContext.listType);
            assertEquals(event.lastContext.playlist, event.nextContext.playlist);
            assertEquals(event.lastContext.station, event.nextContext.station);
            assertEquals(event.lastContext.artist, event.nextContext.artist);
            assertEquals(event.lastContext.album, event.nextContext.album);
            assertEquals(event.lastContext.song, event.nextContext.song);
            // should happen in the middle of playing the last song
            if (lastEventTime > 0)
                assertTrue(event.timestamp - lastEventTime <= SongList.getLengthFor(event.lastContext.song) * 1000 && event.timestamp - lastEventTime >= 0);
        } else if (event.songEventType == SongEventType.Resume) {
            assertEquals(event.lastContext.listType, event.nextContext.listType);
            assertEquals(event.lastContext.playlist, event.nextContext.playlist);
            assertEquals(event.lastContext.station, event.nextContext.station);
            assertEquals(event.lastContext.artist, event.nextContext.artist);
            assertEquals(event.lastContext.album, event.nextContext.album);
            assertEquals(event.lastContext.song, event.nextContext.song);
            // should happen between 30 seconds and 20 minutes later
            if (lastEventTime > 0)
                assertTrue(event.timestamp - lastEventTime < 1200000 && event.timestamp - lastEventTime >= 30000);
        }
    }

    private void verifyContext(SongEvent.Context context) {
        assertNotNull(context.listType);
        if (context.listType == SongEvent.ListType.SingleSong || context.listType == SongEvent.ListType.Album) {
            assertNull(context.playlist);
            assertNull(context.station);
        } else if (context.listType == SongEvent.ListType.Playlist) {
            assertNotNull(context.playlist);
            assertNull(context.station);
        } else if (context.listType == SongEvent.ListType.Station) {
            assertNotNull(context.station);
            assertNull(context.playlist);
        }
        assertNotNull(context.artist);
        assertNotNull(context.album);
        assertNotNull(context.song);
    }
}
