/**
 * ENTRY FILE FOR TRANSFOMRATION
 */
import fs from "fs";
import csv from "fast-csv";
import { dir } from "./utils/platform.js";
import { Artist } from "./models/Artist.js";
import { Track, TransformedTrack } from "./models/Track.js";
import path from "path";

const loadCSV = (filePath: string): Promise<any[]> => {
    return new Promise((resolve, reject) => {
        const data: any[] = [];

        fs.createReadStream(filePath)
            .pipe(csv.parse({ headers: false }))
            .on('error', error => reject(error))
            .on('data', (row: any[]) => {
                data.push(row);
            })
            .on('end', () => resolve(data));
    });
};

function parseStringArray(str: string) {
    try {
        // Check if the string is in a valid format
        if (typeof str !== 'string' || !str.startsWith('[') || !str.endsWith(']')) {
            throw new Error('Invalid input format');
        }

        // Remove the brackets from the string
        str = str.slice(1, -1);

        // Split the string into an array of strings
        let arr = str.split(',').map(item => {
            item = item.trim();
            if (item.charAt(0) === "'") {
                item = item.slice(1);
            }
            if (item.charAt(item.length - 1) === "'") {
                item = item.slice(0, -1);
            }
            return item;
        });

        return arr;
    } catch (err) {
        console.warn("Unable to parse '%s' input: %s", str, (err as Error).message);
        return []; // Return an empty array if parsing fails
    }
}

async function transformData() {
    //  const [tracksData, artistsData] = await Promise.all([
    //     loadCSV('tracks.csv'),
    //     loadCSV('artists.csv')
    // ]);

    const artistsMap: Record<string, Artist> = {};
    const artistTracksMap: Record<string, TransformedTrack[]> = {};

    console.log("Transforming tracks. This might take a while...");
    const tracksData = await loadCSV(
        path.resolve(dir, "..", "artifacts", "tracks.csv")
    );
    console.log("Loaded dataset containing %d rows.", tracksData.length);

    for (let row of tracksData) {
        // Must be first header containing no ID.
        if (!row[0] || row[1] == "0") {
            continue;
        }

        const track: Track = {
            id: row[0],
            name: row[1],
            popularity: parseInt(row[2], 10),
            duration: parseInt(row[3], 10),
            explicit: row[4] === '1',
            artists: parseStringArray(row[5]),
            id_artists: parseStringArray(row[6]),
            release_date: row[7],
            danceability: parseFloat(row[8]),
            energy: parseFloat(row[9]),
            key: parseInt(row[10], 10),
            loudness: parseFloat(row[11]),
            mode: parseInt(row[12], 10),
            speechiness: parseFloat(row[13]),
            acousticness: parseFloat(row[14]),
            instrumentalness: parseFloat(row[15]),
            liveness: parseFloat(row[16]),
            valence: parseFloat(row[17]),
            tempo: parseFloat(row[18]),
            time_signature: parseInt(row[18], 10),
        };

        // Ignore the tracks that have no name; (track.name)
        // Ignore the tracks shorter than 1 minute; (track.duration >= 60000)
        if (track.name && track.duration >= 60000) {
            // Explode track release date into separate columns: year, month, day.
            const [year, month, day] = track.release_date.split('-').map(Number);

            let danceability: string;
            // Transform track danceability into string values based on these intervals:
            if (track.danceability < 0.5) {
                danceability = 'Low';
            } else if (track.danceability <= 0.6) {
                danceability = 'Medium';
            } else {
                danceability = 'High';
            }

            const transformedTrack: TransformedTrack = {
                ...track,
                year,
                month: !month ? null : month,
                day: !day ? null : day,
                danceability,
            };
            // @ts-ignore
            delete transformedTrack.release_date;

            track.id_artists.forEach(artistId => {
                if (!artistTracksMap[artistId]) {
                    artistTracksMap[artistId] = [];
                }
                artistTracksMap[artistId].push(transformedTrack);
            });
        }
    }

    console.log("Transforming artists. This might take a while...");
    const artistsData = await loadCSV(
        path.resolve(dir, "..", "artifacts", "artists.csv")
    );
    console.log("Loaded dataset containing %d rows.", artistsData.length);
    
    artistsData.forEach(row => {
        const artist: Artist = {
            id: row[0],
            followers: parseInt(row[1], 10),
            genres: row[2],
            name: row[3],
            popularity: parseInt(row[4], 10),
        };

        if (artistTracksMap[artist.id]) {
            artistsMap[artist.id] = artist;
        }
    });

    console.log("Done!");
    console.log(artistsMap);
}

(async () => {
    console.log("Kaggle BigData Tool");
    await transformData();

})();
