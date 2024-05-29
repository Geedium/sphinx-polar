import fs from "fs";
import csv from "fast-csv";

export async function loadCSV(filePath: string): Promise<any[]> {
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
}
