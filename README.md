# 🏏 Snowflake End-to-End Real-Time Cricket Analytics Project

This project demonstrates an end-to-end data engineering solution using Snowflake for processing a large volume of deeply nested JSON data (Cricket match statistics) into a structured data warehouse for real-time analytics.

The solution implements a tiered ELT architecture (Landing, Raw, Clean, Consumption) and leverages Snowflake's unique features like **Stages, Variant Data Types, FLATTEN(), Streams, and Tasks (DAG)** for automated data pipelines.

## ⚙️ Architecture and Data Flow

The project follows a standard medallion-style (Zone-based) architecture:

| Layer | Schema | Data Type | Purpose |
| :---: | :---: | :---: | :--- |
| **Landing** | `CRICKET.LAND` | JSON/Variant | Stores raw, unprocessed JSON files in an internal Stage location. |
| **Raw** | `CRICKET.RAW` | Variant, Object, Array | Initial load of the entire JSON object into a single table (`MATCH_RAW_TBL`). The source of truth for all data. |
| **Clean** | `CRICKET.CLEAN` | Structured | Flattened, cleansed, and standardized data. Contains granular tables like `MATCH_DETAIL_CLEAN`, `PLAYER_CLEAN`, and `DELIVERY_CLEAN`. |
| **Consumption** | `CRICKET.CONSUMPTION` | Structured (Star Schema) | Analytical layer with Fact and Dimension tables ready for BI and reporting. |




## 🛠️ Technology Stack

* **Cloud Data Warehouse:** Snowflake
* **Core Languages:** SQL (DDL, DML, CTAS)
* **Key Snowflake Features:** Stages, File Formats, `FLATTEN()`, Streams, Tasks (DAG), Variant/Object/Array Data Types.
* **Data Source:** Deeply Nested Cricket Match JSON files.

## 🚀 Project Steps & Repository Structure

The project is structured into modular SQL scripts executed sequentially to build the pipeline:

| Folder/File | Description |
| :---: | :--- |
| `01_Setup/01_Schema_Stage_Setup.sql` | Creates the Database, Schemas, JSON File Format, and Internal Stage. |
| `02_Raw_Layer/02_Raw_Layer_Load.sql` | Creates the `MATCH_RAW_TBL` and loads raw JSON data from the stage using `COPY INTO`. |
| `03_Clean_Layer/03_Clean_Layer_Match_Player.sql` | Flattens Match and Player elements into structured clean tables. |
| `03_Clean_Layer/04_Clean_Layer_Delivery.sql` | Uses multiple nested `FLATTEN` and `LATERAL` functions to extract ball-by-ball (delivery) data. |
| `04_Consumption_Layer/05_Consumption_Dim_Fact.sql` | Creates and populates all Dimension and Fact tables (`DIM_DATE`, `DIM_TEAM`, `FACT_MATCH`, `FACT_DELIVERY`). |
| `05_Automation/06_Stream_Task_Automation.sql` | Sets up **Streams** on the Raw table and a **Task DAG** to automate the full ELT pipeline for continuous data loading. |
| `data/` | Placeholder for sample JSON files. (Due to size, please obtain the full dataset from the video description). |

## 💾 Setting up the Project

### Prerequisites

1.  A Snowflake account (Enterprise Edition recommended for advanced features).
2.  SnowSQL CLI or Snowsight Web UI access.
3.  The complete set of Cricket Match JSON files.

### Data Ingestion (Initial Load)

1.  **Execute Setup Script:** Run `01_Setup/01_Schema_Stage_Setup.sql`.
2.  **Upload Data:** Upload the JSON files to the internal stage using the Snowsight Web UI loader or the `PUT` command via SnowSQL CLI:
    ```bash
    # Example using SnowSQL CLI (replace path)
    PUT file://<local_json_path>/*.json @CRICKET.LAND.MY_STAGE PARALLEL=50;
    ```
3.  **Load Raw Data:** Execute `02_Raw_Layer/02_Raw_Layer_Load.sql` to copy data from the stage into the raw table.

### Transformation & Modeling

Execute the remaining scripts in order:

1.  `03_Clean_Layer/03_Clean_Layer_Match_Player.sql`
2.  `03_Clean_Layer/04_Clean_Layer_Delivery.sql`
3.  `04_Consumption_Layer/05_Consumption_Dim_Fact.sql`

### Real-Time Pipeline Automation

Execute `05_Automation/06_Stream_Task_Automation.sql`. This script sets up the automated DAG. Once complete, any new JSON files added to the internal stage will automatically flow through the entire ELT pipeline every 5 minutes.
