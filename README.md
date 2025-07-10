# Data lake

The goal of this work is create a simple end-to-end data architecture, including data ingestion, data transformation and data exposition, which will permit us to structure the data correctly, to have a clean data pipeline and shareable data.

![Schema global](/assets/images/schema_structure.png)

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

## Data transformation

![Data transformation](/assets/images/data_transformation.png)

## Combination data

![Data combination](/assets/images/data_combination.png)

## Indexing data

![Data indexing](/assets/images/data_indexing.png)

## Data visualization

![Data ingestion](/assets/images/data_visualization.png)