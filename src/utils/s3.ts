import fs from "fs";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { Readable } from "stream";

export async function uploadToS3(client: S3Client, bucket: string, key: string, data: Readable): Promise<any> {
    const command = new PutObjectCommand({ Bucket: bucket, Key: key, Body: data });

    try {
        const upload = new Upload({ client, params: command.input });
        return await upload.done();
    } catch (err) {
        throw new Error(`Failed to upload file to S3: ${(err as Error).message}`);
    }
}

export async function downloadFromS3(client: S3Client, bucket: string, key: string, downloadPath: string) {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await client.send(command);

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