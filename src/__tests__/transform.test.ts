// Mock the dependencies
jest.mock('../utils/csv', () => jest.fn());
jest.mock('../utils/s3', () => jest.fn());

describe('Data Transformation', () => {
    beforeEach(() => {
        // Clear all mocks between tests
        jest.clearAllMocks();
    });

    it('should transform data and upload to S3', async () => {
        const csv = require('../utils/csv');
        csv.loadCSV.mockResolvedValueOnce([
            ['id', 'name', 'popularity', 'duration', 'explicit', 'artists', 'id_artists', 'release_date', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'],
            ['1', 'Track 1', '80', '300000', '1', 'Artist 1', '1', '2022-01-01', '0.7', '0.8', '2', '-5', '1', '0.1', '0.2', '0.3', '0.4', '0.5', '120', '4'],
            ['2', 'Track 2', '90', '200000', '0', 'Artist 2', '2', '2022-01-02', '0.6', '0.7', '3', '-4', '0', '0.2', '0.3', '0.4', '0.5', '0.6', '130', '4']
        ]);
        csv.loadCSV.mockResolvedValueOnce([
            // Mock artists data
            ['id', 'followers', 'genres', 'name', 'popularity'],
            ['1', '1000', 'Pop', 'Artist 1', '80'],
            ['2', '2000', 'Rock', 'Artist 2', '90']
        ]);

        const uploadToS3 = require('../utils/s3');

        // Call the function
        // await transformData();

        // Assert that loadCSV was called twice, once for tracks and once for artists
        expect(csv.loadCSV).toHaveBeenCalledTimes(2);

        // Assert that uploadToS3 was called twice, once for tracks and once for artists
        expect(uploadToS3.uploadToS3).toHaveBeenCalledTimes(2);

        // Assert that uploadToS3 was called with the correct arguments
        expect(uploadToS3).toHaveBeenCalledWith(
            expect.any(String), // S3_BUCKET_NAME
            expect.any(String), // S3_TRACKS_KEY
            expect.any(Object) // transformedTracksStream
        );

        expect(uploadToS3).toHaveBeenCalledWith(
            expect.any(String), // S3_BUCKET_NAME
            expect.any(String), // S3_ARTISTS_KEY
            expect.any(Object) // transformedArtistsStream
        );
    });
});