import fs from "fs";
import http from "http";
import https from "https";
import { URL } from "url";

export function getRemoteFile(file: fs.PathLike, url: string, cb: { (): void; (): void; }) {
    let localFile = fs.createWriteStream(file);
    const parsedUrl = new URL(url);
    const protocol = parsedUrl.protocol === "https:" ? https : http;

    const req = protocol.get(url, function (response) {
        var len = parseInt(response.headers["content-length"] || "", 10);
        var cur = 0;
        var total = len / 1048576; // 1048576 bytes in 1 Megabyte

        response.on("data", function (chunk) {
            cur += chunk.length;
            showProgress(file, cur, len, total);
        });

        response.on("error", (err) => {
            console.error(err);
        });

        response.on("end", function () {
            console.log("Download complete");
            cb();
        });

        response.pipe(localFile);
    });

    req.on("error", (err) => {
        console.error(`Request error: ${err.message}`);
    });
}

export function showProgress(file: fs.PathLike, cur: number, len: number, total: number) {
    console.log("Downloading " +
        file +
        " - " +
        ((100.0 * cur) / len).toFixed(2) +
        "% (" +
        (cur / 1048576).toFixed(2) +
        " MB) of total size: " +
        total.toFixed(2) +
        " MB"
    );
}
