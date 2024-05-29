export interface Track {
    id: string; // @string
    name: string; // @string
    popularity: number; // @int
    duration: number; // @int (ms.)
    explicit: boolean; // @bool (0/1)
    artists: string; // @string[]
    id_artists: string; // @string[]
    release_date?: string; // @string(date yyyy-mm-dd)
    danceability: number; // @float
    energy: number; // @float
    key: number; // @int
    loudness: number; // @float
    mode: number; // @int
    speechiness: number; // @float
    acousticness: number; // @float
    instrumentalness: number; // @float
    liveness: number; // @float
    valence: number; // @float
    tempo: number; // @float
    time_signature: number; // @int
}

type OmitFields = 'release_date' | 'danceability' | 'id_artists' | 'artists';

export interface TransformedTrack extends Omit<Track, OmitFields> {
    year: number; // @int
    month: number | null; // @int
    day: number | null; // @int
    danceability: string; // @string(Low,Medium,High) Enum
    artists: string[];
    id_artists: string[];
}