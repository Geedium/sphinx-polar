import path from "path";

const args = process.argv;
export const dir = path.join(args[1], process.platform === "win32" ? "..\\" : "../");
