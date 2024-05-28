/**
 * This will use Official Kaggle CLI to retrieve data,
 * Python must be installed to interact with Kaggle.
 */
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from "fs";
import path from "path";
import unzipper from "unzipper";

const DATASET_OWNER = "yamaerenay";
const DATASET_NAME = "spotify-dataset-19212020-600k-tracks";
const DATASET_PATH = `${DATASET_OWNER}/${DATASET_NAME}`

const args = process.argv;
const dir = path.join(args[1], process.platform === "win32" ? "..\\" : "../");

const execPromise = promisify(exec);

async function executeCommand(command) {
    try {
        const { stdout, stderr } = await execPromise(command);
        return stdout || stderr;
    } catch (error) {
        throw error;
    }
}

async function checkPython() {
    try {
        await executeCommand('python --version');
        console.log('Python is installed.');
        return true;
    } catch (error) {
        console.error('Python is not installed.');
        return false;
    }
}

async function checkKagglePackage() {
    try {
        await executeCommand('pip show kaggle');
        console.log('Kaggle package is already installed.');
        return true;
    } catch (error) {
        console.log('Kaggle package is not installed.');
        return false;
    }
}

async function installKagglePackage() {
    try {
        const result = await executeCommand('pip install kaggle');
        console.log('Kaggle package installed successfully:', result);
    } catch (error) {
        console.error('Failed to install Kaggle package:', error);
    }
}

async function downloadDatasetFiles(dataset) {
    try {
        const result = await executeCommand(`kaggle datasets download -d ${dataset}`);
        console.log(`Files for dataset "${dataset}" downloaded successfully:`, result);
    } catch (error) {
        console.error(`Failed to download files for dataset "${dataset}":`, error);
    }
}

async function unzipDatasetFiles() {
    console.log(`Unzipping files for dataset "${DATASET_PATH}...`);
    const zipDest = path.resolve(dir, `${DATASET_NAME}.zip`);
    return new Promise((resolve, reject) => {
        fs.createReadStream(zipDest)
        .pipe(unzipper.Extract({ path: path.resolve(dir, "artifacts") }))
        .on("finish", function (err) {
            if (err) {
                reject(err);
                return;
            }
            fs.unlinkSync(zipDest);
            console.log("Unzip completed.");
            resolve();
        });
    })
}

async function main(dataset) {
    const isPythonInstalled = await checkPython();
    if (!isPythonInstalled) {
        console.error('Please install Python before running this script.');
        return;
    }

    const isKaggleInstalled = await checkKagglePackage();
    if (!isKaggleInstalled) {
        await installKagglePackage();
    }

    await downloadDatasetFiles(dataset);

    await unzipDatasetFiles();
}

main(DATASET_PATH);
