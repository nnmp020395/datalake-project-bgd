# Data lake

The goal of this work is create a simple end-to-end data architecture, including data ingestion, data transformation and data exposition, which will permit us to structure the data correctly, to have a clean data pipeline and shareable data.

![Schema global](/assets/images/schema_structure.png)

Throughout the project, Airfloww was deployed to orchestrate our operations

## Datasets
### Realtime dataset

The concentration of atmospheric polluants in realtime are used in this project. These measureaments are collected throughout France, controlled by 18 approuved surveillance associations and distribubted to the public via the Central Surveillance Laboratory. They are also available on the government data center and they are updated very regularly, update every hour.

- In 2022, 585 stations in France will be measuring air pollutants.
- Three types of locations are available, depending on the station's environment: urban, peri-urban, and rural.
- Three main types of influences: industrial, background, and traffic.
- Real-time data updated every hour.

![Dataset](/assets/images/dataset.png)

### France map

## Data ingestion

![Data ingestion](/assets/images/data_ingestion.png)

Two ingestion tasks worked in parallel for getting the realtime dataset and gps mapping. On this step, any transformation method was applied. Data was stockked on AWS Bucket S3

## Data transformation

![Data transformation](/assets/images/data_transformation.png)

Two extraction tasks worked in parallel but the formatting tasks for each dataset worked independently and sequentially after their extraction tasks. Dataset was convetted in parquet format and arranged by polluants.

## Combination data

![Data combination](/assets/images/data_combination.png)

Create before indexes on ElesticSearch, after send all datasets to ElasticSearch.

## Indexing data

![Data indexing](/assets/images/data_indexing.png)

## Data visualization

![Data ingestion](/assets/images/data_visualization.png)