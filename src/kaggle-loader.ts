class Kaggle {
    static instance: Kaggle | undefined;

    constructor() {
        if (!Kaggle.instance) {
            Kaggle.instance = new Kaggle();
            console.log("Instantiated new Kaggle instance");
        }
    }

    /**
     * @remarks ingest the data from the source
     */
    static download(sampleURL: string) {
        
    }
}

export default Kaggle;
