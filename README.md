# Ingestion Pipelines Manager

This is a PoC of one of the ingestion pipelines of the Data & Analytics Framework. It takes as input a Spark Dataframe or a logical uri of the dataset and a list of pipelines to be applied and returns a Spark DataFrame and a class containing info about the operations performed. After all the pipelines have been applied, it saves the DataFrame in the folder 'ingPipelines', whose content will be then saved into the associated DAF dataset. Currently, there is only one pipeline developed, the Standardization Pipeline. 

## Standardization Pipeline
Given a newly ingested dataset, it uses metadata information linked with ontologies and controlled vocabularies to check if the values in the dataset are compliant with the controlled vocabularies. It augments the dataset with two columns per each column that has a controlled vocabulary associated:

* '__std_{colname}': it contains the value of the column {colname} after the standardization procedure. Each value will be the same value of {colname} in case it is contained in the controlled vocabulary or the closest one (using Levenshtein distance);

* '__stdstat_{colname}': it contains a measure of the distance between the original value and closest one found in the controlled vocabulariy.

## How to run
To run the PoC, you need to setup and run two external components, the Semantic Manager and the Catalog Manager.

### Semantic Manager
1. Download and unzip `https://github.com/italia/daf-semantics/releases/download/0.0.1-TESTING/semantic_standardization-0.0.1-testing.zip`

2. Import the docker image `docker load -i semantic_standardization-0.0.1.tar`

3. Get the {IMAGE_ID} `docker images`

4. Run the docker image `docker run -p 9005:9000 {IMAGE_ID}`

### Catalog Manager
1. Download `https://github.com/italia/daf/tree/dev-ingestionmgr/catalog_manager`

2. Go into the `catalog_manager` folder and run `sbt run`

### Pipeline Ingestion
1. Go in the project folder and run `sbt run` and select the class `ingestion.pipelines.IngestionProc`

2. A new directory `data/ingPipelines` will be created with the final dataset.
