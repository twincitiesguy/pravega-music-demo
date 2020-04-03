package com.dellemc.sdp.demo.music;

public enum SongEventType {
    Select, // selecting a station, playlist, album or song
    Next, // player went to the next song in the context after the last song finished
    Skip, // user skipped this song to go to next song in the current playlist
    Pause,
    Resume
}
