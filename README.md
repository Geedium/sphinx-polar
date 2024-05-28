# Kaggle Big Data Dataset Tool
Filters, optimizes CSV files arists and tracks to easier generate claim and saves records after filtering to SQL database.

This tool will require you to have ~user/.kaggle/kaggle.json config configured before running the tool. You can obtain your configuration in their  [Public API](https://www.kaggle.com/docs/api). This also emphasize that you have Python already installed on your system to run CLI to successfully extract CSVs.

# Prerequisites
 - Python
 - Node.js
 - PostgreSQL

# Installing Node.js Dependencies
This step is relatively simple and self explanatory.
```bash
yarn or npm install or <YOUR_PACKAGE_MANAGER>
```
There are also alternatives like **pnpm**.

# Running Phase
```bash
yarn bake
```
This command will get CSV files associated with the artist and track.

```bash
yarn dev
```
After this command there should be **dist** folder and desired results should appear.
