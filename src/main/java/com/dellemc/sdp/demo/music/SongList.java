package com.dellemc.sdp.demo.music;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

public final class SongList {
    private static final String SONG_MAP_RESOURCE = "/songs.lst";
    private static final SplittableRandom random = new SplittableRandom();

    private static List<String> _songList;
    private static Map<String, String> _artistMap;
    private static Map<String, Integer> _lengthMap;

    static String getArtistFor(String song) {
        return getArtistMap().get(song);
    }

    static Integer getLengthFor(String song) {
        return getLengthMap().get(song);
    }

    static String getRandomSong() {
        return getSongList().get(random.nextInt(getSongList().size()));
    }

    private static Map<String, String> getArtistMap() {
        if (_artistMap == null) {
            synchronized (SongEventGenerator.class) {
                if (_artistMap == null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(SongList.class.getResourceAsStream(SONG_MAP_RESOURCE)));
                    _artistMap = reader.lines().collect(Collectors.toMap(s -> s.split("::")[0], s -> s.split("::")[1]));
                }
            }
        }
        return _artistMap;
    }

    private static Map<String, Integer> getLengthMap() {
        if (_lengthMap == null) {
            synchronized (SongEventGenerator.class) {
                if (_lengthMap == null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(SongList.class.getResourceAsStream(SONG_MAP_RESOURCE)));
                    _lengthMap = reader.lines().collect(Collectors.toMap(s -> s.split("::")[0], s -> Integer.parseInt(s.split("::")[2])));
                }
            }
        }
        return _lengthMap;
    }

    private static List<String> getSongList() {
        if (_songList == null) {
            synchronized (SongEventGenerator.class) {
                if (_songList == null) {
                    _songList = new ArrayList<>(getArtistMap().keySet());
                }
            }
        }
        return _songList;
    }

    private SongList() {
    }
}
