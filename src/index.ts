/**
 * ENTRY FILE FOR TRANSFOMRATION
 */
import fs from "fs";
import csv from "fast-csv";
import { dir } from "./utils/platform.js";
import { Artist } from "./models/Artist.js";
import { Track, TransformedTrack } from "./models/Track.js";
import path from "path";

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { Client } = require('pg');

import 'dotenv/config';
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from '@aws-sdk/lib-storage';

import { Command } from 'commander';
import { Readable } from "stream";


const S3_BUCKET_NAME = 'sphinx-polar';
const S3_TRACKS_KEY = 'transformed_tracks.csv';
const S3_ARTISTS_KEY = 'transformed_artists.csv';

const s3Client = new S3Client({
    region: process.env.AWS_REGION,
});

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

async function uploadToS3(bucket: string, key: string, data: Readable): Promise<any> {
    const command = new PutObjectCommand({ Bucket: bucket, Key: key, Body: data });

    try {
        const upload = new Upload({
            client: s3Client,
            params: command.input,
        });

        const result = await upload.done();

        return result;
    } catch (err) {
        throw new Error(`Failed to upload file to S3: ${(err as Error).message}`);
    }
}

async function downloadFromS3(bucket: string, key: string, downloadPath: string) {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(command);

    // Check if the Body is a readable stream
    if (!response.Body || !(response.Body instanceof Readable)) {
        throw new Error('Response body is not a readable stream');
    }

    return new Promise<void>((resolve, reject) => {
        const fileStream = fs.createWriteStream(downloadPath);
        (response.Body as Readable).pipe(fileStream);
        (response.Body as Readable).on('error', reject);
        fileStream.on('close', resolve);
    });
}

function parseStringArray(str: string) {
    try {
        if (!str) {
            return [];
        }
        if (typeof str === 'string' && str.trim() === '') {
            return [];
        }

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
        console.warn("Warning: unable to parse '%s' input: %s", str, (err as Error).message);
        return []; // Return an empty array if parsing fails
    }
}

async function transformData() {
    const artistTracksMap: Record<string, TransformedTrack[]> = {};

    console.log("Transforming tracks. This might take a while...");
    const tracksData = await loadCSV(
        path.resolve(dir, "..", "artifacts", "tracks.csv")
    );
    console.log("Loaded dataset containing %d rows.", tracksData.length);

    const transformedTracksStream = csv.format({ headers: true });

    for (const row of tracksData) {
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
            artists: row[5],
            id_artists: row[6],
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

        const id_artists = parseStringArray(track.id_artists);

        // Ignore the tracks that have no name; (track.name)
        // Ignore the tracks shorter than 1 minute; (track.duration >= 60000)
        if (track.name && track.duration >= 60000) {
            // Explode track release date into separate columns: year, month, day.
            const [year, month, day] = track.release_date!.split('-').map(Number);
            delete track.release_date;

            let danceability: string;
            // Transform track danceability into string values based on these intervals:
            if (track.danceability < 0.5) {
                danceability = 'Low';
            } else if (track.danceability <= 0.6) {
                danceability = 'Medium';
            } else {
                danceability = 'High';
            }

            const transformedTrack: any = {
                ...track,
                year,
                month: !month ? null : month,
                day: !day ? null : day,
                danceability,
            };
            id_artists.forEach(artistId => {
                if (!artistTracksMap[artistId]) {
                    artistTracksMap[artistId] = [];
                }
                transformedTracksStream.write(transformedTrack);
            });
        }
    }

    transformedTracksStream.end();

    console.log("Transforming artists. This might take a while...");
    const artistsData = await loadCSV(
        path.resolve(dir, "..", "artifacts", "artists.csv")
    );
    console.log("Loaded dataset containing %d rows.", artistsData.length);

    const transformedArtistsStream = csv.format({ headers: true });

    artistsData.forEach(row => {
        const artist: Artist = {
            id: row[0],
            followers: parseInt(row[1], 10),
            genres: row[2],
            name: row[3],
            popularity: parseInt(row[4], 10),
        };

        if (artistTracksMap[artist.id]) {
            transformedArtistsStream.write(artist);
        }
    });

    transformedArtistsStream.end();

    await Promise.all([
        uploadToS3(S3_BUCKET_NAME, S3_TRACKS_KEY, transformedTracksStream),
        uploadToS3(S3_BUCKET_NAME, S3_ARTISTS_KEY, transformedArtistsStream)
    ]);

    console.log("Done, data transformation succeeded, check AWS S3!");
}

async function fetchFromS3() {
    try {
        console.log("Pulling data from S3. This might take a while...");
        await downloadFromS3(S3_BUCKET_NAME, S3_TRACKS_KEY, path.resolve(dir, "..", "artifacts", "tracks.csv"));
        await downloadFromS3(S3_BUCKET_NAME, S3_ARTISTS_KEY, path.resolve(dir, "..", "artifacts", "artists.csv"));
    } catch (err) {
        console.error('Error processing data:', err);
    }
}

async function eraseTables() {
    const client = new Client({
        user: process.env.POSTGRE_USER,
        host: process.env.POSTGRE_HOST,
        database: process.env.POSTGRE_DATABASE,
        password: process.env.POSTGRE_PASS,
        port: 5432,
    });

    try {
        await client.connect();
        console.log("Connection to Postgre server is established.");

        console.log("Dropping tables if any exists...")
        await client.query("DROP TABLE IF EXISTS tracks");
        await client.query("DROP TABLE IF EXISTS artists");
        console.log("Successfully dropped tables.");
    } catch (err) {
        console.error('Error processing data:', err);
    } finally {
        client.end();
    }
}

async function createRecords() {
    const client = new Client({
        user: process.env.POSTGRE_USER,
        host: process.env.POSTGRE_HOST,
        database: process.env.POSTGRE_DATABASE,
        password: process.env.POSTGRE_PASS,
        port: 5432,
    });

    const insertRecordsInBatches = async (tableName: string, records: any[], batchSize: number) => {
        try {
            for (let i = 0; i < records.length; i += batchSize) {
                const batch = records.slice(i, i + batchSize);
                const columns = Object.keys(batch[0]);
                const values = batch.map(Object.values);

                const placeholders = batch.map((_, j) => `(${columns.map((_, k) => `$${j * columns.length + k + 1}`).join(', ')})`).join(', ');

                const query = `
                    INSERT INTO ${tableName} (${columns.join(', ')})
                    VALUES ${placeholders}
                    ON CONFLICT (id) DO NOTHING;
                `;

                await client.query(query, values.flat());
            }

            console.log(`Records inserted into ${tableName} successfully.`);
        } catch (error) {
            console.error(`Error inserting records into ${tableName}:`, error);
        }
    };

    try {
        await client.connect();
        console.log("Connection to Postgre server is established.");

        // Note: It is also possible to load SQL queries from FS (.sql)

        await client.query(`
        CREATE TABLE IF NOT EXISTS artists (
            id VARCHAR PRIMARY KEY,
            followers FLOAT,
            genres TEXT[],
            name VARCHAR,
            popularity INT
        );
        `);
        console.log("Created artists table.");

        await client.query(`
        CREATE TABLE IF NOT EXISTS tracks (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            popularity INT,
            duration INT,
            explicit BOOLEAN,
            artist_id VARCHAR REFERENCES artists(id),
            danceability VARCHAR,
            energy FLOAT,
            key INT,
            loudness FLOAT,
            mode INT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INT,
            year INT,
            month INT,
            day INT
        );
        `);
        console.log("Created tracks table.");

        console.log("Loading artists from artifacts. This might take a while...");
        const artistsData = await loadCSV(
            path.resolve(dir, "..", "artifacts", "artists.csv")
        );
        console.log("Loaded dataset containing %d rows.", artistsData.length);

        const artists: Artist[] = [];
        const artistIds = new Set<string>(); 
        for (const row of artistsData) {
            // Must be first header containing no ID.
            if (!row[0] || row[0] == "id") {
                continue;
            }

            const artist: Artist = {
                id: row[0],
                followers: parseInt(row[1], 10),
                genres: parseStringArray(row[2]),
                name: row[3],
                popularity: parseInt(row[4], 10),
            };
            artists.push(artist);
            artistIds.add(artist.id);
        }
        console.log("Inserting records. It will take a while...");
        await insertRecordsInBatches('artists', artists, 500);
        console.log("Artists loaded successfully.");

        console.log("Loading tracks from artifacts. This might take a while...");
        const tracksData = await loadCSV(
            path.resolve(dir, "..", "artifacts", "tracks.csv")
        );
        console.log("Loaded dataset containing %d rows.", tracksData.length);

        const tracks: any[] = [];

        const safeParseInt = (value: string): number => {
            try {
                // Attempt to parse the value to an integer
                const intValue = parseInt(value, 10);
                // If parsing succeeds, return the parsed integer value
                // If the parsed value is NaN or empty string, return 0
                return isNaN(intValue) ? 0 : intValue;
            } catch (error) {
                // If an error occurs during parsing, return 0
                return 0;
            }
        };

        console.log("Preparing data for insertion... (up to 20 minutes)...");
        for (let row of tracksData) {
            // Must be first header containing no ID.
            if (!row[0] || row[0] == "id") {
                continue;
            }

            const year = safeParseInt(row[19]);
            const month = safeParseInt(row[20]);
            const day = safeParseInt(row[21]);
            const id_artists = parseStringArray(row[6]);

            // Create a separate track record for each artist_id (many-to-many)
            for (let i = 0; i < id_artists.length; i++) {
                if (artistIds.has(id_artists[i])) {
                    tracks.push({
                        id: row[0],
                        name: row[1],
                        popularity: safeParseInt(row[2]),
                        duration: safeParseInt(row[3]),
                        explicit: row[4] === '1',
                        danceability: row[7],
                        energy: parseFloat(row[8]),
                        key: safeParseInt(row[9]),
                        loudness: parseFloat(row[10]),
                        mode: safeParseInt(row[11]),
                        speechiness: parseFloat(row[12]),
                        acousticness: parseFloat(row[13]),
                        instrumentalness: parseFloat(row[14]),
                        liveness: parseFloat(row[15]),
                        valence: parseFloat(row[16]),
                        tempo: parseFloat(row[17]),
                        time_signature: safeParseInt(row[18]),
                        year, month, day,
                        artist_id : id_artists[i],
                    });
                }
            }
        }

        console.log("Inserting records. It will take a while...");
        await insertRecordsInBatches('tracks', tracks, 500);
        console.log("Tracks loaded successfully.");
    } catch (err) {
        console.error('Error processing data:', err);
    } finally {
        await client.end();
    }
}

async function createViews() {
    const client = new Client({
        user: process.env.POSTGRE_USER,
        host: process.env.POSTGRE_HOST,
        database: process.env.POSTGRE_DATABASE,
        password: process.env.POSTGRE_PASS,
        port: 5432,
    });

    try {
        await client.connect();

        // Create track_info view
        await client.query(`
            CREATE OR REPLACE VIEW track_info AS
            SELECT
                t.id AS track_id,
                t.name AS track_name,
                t.popularity,
                t.energy,
                t.danceability,
                a.followers AS artist_followers,
                t.year,
                t.month,
                t.day
            FROM
                tracks t
            JOIN
                artists a ON t.artist_id = a.id;
        `);

        console.log('View track_info created successfully.');

        // Create tracks_with_artist_followers view
        await client.query(`
            CREATE OR REPLACE VIEW tracks_with_artist_followers AS
            SELECT
                *
            FROM
                track_info
            WHERE
                artist_followers > 0;
        `);

        console.log('View tracks_with_artist_followers created successfully.');

        // Create most_energizing_tracks view
        await client.query(`
            CREATE OR REPLACE VIEW most_energizing_tracks AS
            SELECT
                t.year,
                t.track_id,
                t.track_name,
                t.popularity,
                t.energy,
                t.danceability,
                t.artist_followers
            FROM
                tracks_with_artist_followers t
            JOIN (
                SELECT
                    year,
                    MAX(energy) AS max_energy
                FROM
                    tracks_with_artist_followers
                GROUP BY
                    year
            ) max_energy_tracks ON t.year = max_energy_tracks.year AND t.energy = max_energy_tracks.max_energy;
        `);

        console.log('View most_energizing_tracks created successfully.');

    } catch (err) {
        console.error('Error creating views:', err);
    } finally {
        await client.end();
    }
}

(async () => {
    console.log("Kaggle BigData Tool - CLI");
    const program = new Command();
    program
        .option('-f, --fetch', 'Fetches data from S3')
        .option('-t, --transform', 'Transform, optimize, clean CSV data')
        .option('-c, --create', 'Create records on PostgreSQL')
        .option('-d, --delete', 'Delete records on PostgreSQL')
        .option('-v, --views', 'Create views on PostgreSQL')
        .parse(process.argv);

    const options = program.opts();
    if (options.transform) {
        await transformData();
    }
    if (options.fetch) {
        await fetchFromS3();
    }
    if (options.delete) {
        await eraseTables();
    }
    if (options.create) {
        await createRecords();
    }
    if (options.views) {
        await createViews();
    }
})();
