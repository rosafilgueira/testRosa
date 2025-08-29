# 🧠 Gateway to Research (GtR): UoE Project Extraction Module

This module is responsible for working with UKRI Gateway to Research (GtR) project data for the **University of Edinburgh (UoE)**. Unlike the Opportunity Match (OM) module, where the data was already available in a pre-parsed format (extracted from a subset of Research Explorer dataset), here we first had to perform a **dedicated extraction step using GtR API v1 and v2**.

This initial retrieval stage produces enriched project records, which we then transform and map into our **SQL schema** (TBA)

This module focuses on **retrieving, enriching, and preparing UKRI Gateway to Research (GtR)** project data for the **University of Edinburgh (UoE)**. The extracted data forms the basis for subsequent parsing and integration into the **Impact Tracker SQL schema**.

## 📂 1. Data Availability

All required input files are available here:

🔗 [GtR UoE Data Files (Google Drive)](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link)

### Key Files

| Filename                        | Use for SQL | Description                                                                    |
|--------------------------------|-------------|--------------------------------------------------------------------------------|
| `gtr_uoe_org_info.json`        | ✅ Yes      | Metadata for the University of Edinburgh as an organization + address fields  |
| `uoe_full_projects_info.jsonl` | ✅ Yes      | Core input file: detailed project metadata (API v1 + v2)                       |
| `uoe_project_ids.json`   | ❌ No  | Project ID list (used in collection step, not parsing)                         |
| `edinburgh_project_metadata.json` | 🟡 Optional | Title, funder, dates — good for validation/debug                              |
| `gtr_organisations.csv`        | ❌ No       | Used to look up org IDs during scraping phase only                            |
| `uoe_subset_projects_info.jsonl` | ❌ No     | Subset of uoe_full_projects_info.jsonl - might be useful for quick inspection   |


Those files are the ones generated in GTR_APIs_Extraction step, so you can access directly to them in the google drive, without the need to execute the scripts/notebook. 

## 📦 2. GtR_APIs_Extraction

Unlike the Opportunity Match module (which provided pre-extracted files via Dropbox), this module **programmatically extracts** and processes the required GtR project metadata. We use a **hybrid API strategy** that combines both:

- **GtR API v1**: to retrieve comprehensive project metadata, outputs, collaborations, and financial info.
- **GtR API v2**: to enrich results with person ORCIDs, publication DOIs, technical abstracts, and health categories.


**Remember**: All the information generated in this section is already available 🔗 [GtR UoE Data Files (Google Drive)](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link) - so you dont have to re-generate it.  

We have the following [notebook](https://colab.research.google.com/drive/1pb8zIj04kW1-wWA11DCdxzNHJRfLVGnN?usp=sharing), which serves two complementary purposes:

- 🧠 **Conceptual overview**: It compares the UKRI GtR API v1 and v2 in detail and outlines a hybrid retrieval strategy.
- 🔍 **Practical exploration**: It demonstrates how to extract and structure project data from the University of Edinburgh using the GtR APIs.

While the notebook is an excellent reference for understanding the APIs and for inspecting data interactively, **we recommend using the two accompanying scripts** (**instead of using the notebook for steps 7 and 8**) to perform large-scale extraction more reliably and efficiently:


### 2.1. **Extract Organisation ID**

Query all organizations via the GtR v1 API and identify the internal ID for the University of Edinburgh: **A4735E0B-ED6E-4FC5-9497-CFE564317F54**. This query generates the following file:

- `gtr_organisations.csv` --> from that one we select this id: **A4735E0B-ED6E-4FC5-9497-CFE564317F54**

After that query, we run another to get all the information (mainly name, postcode, region, addressLine) of the UoE organization (using the previous id)  and save that info into a file: 
- ` gtr_uoe_org_info.json` --> we will use that file in the `future gtr to sql parsers`

### 2.2. **Fetch All Projects (ids, code_ref, basic metadata) for UoE**

Using the UoE ID, we retrieve all ~5,400 projects associated with the institution. For each project, we store:
- Project ID
- Grant Reference (e.g., `EP/Z531170/1`)
- Title, funder, start/end dates

Saved as:
- `uoe_project_ids.json` --> This is used in the next step
- `edinburgh_project_metadata.json`

✅ **Script:** `fetch_all_uoe_projects.py`

### 2.3. **Extract Full Project Information**

For each project in our `[project_id, grant_ref]` list (using `uoe_project_ids.json` file), we retrieve **detailed and enriched metadata** using both **GtR API v1 and v2**.

This step combines:
- 📦 **Core project metadata** from API v1:
  - Title, status, start/end dates
  - Funded value, departments, organizations
  - Collaborators and grant schemes
- 🌐 **Additional enrichments** from API v2:
  - Researcher ORCID identifiers
  - DOIs for publications
  - Technical abstracts and potential impact statements
  - Health research categories (if applicable)

- 📊 **Research outcomes** parsed from both APIs:
  - Collaborations and international partnerships
  - Further funding and follow-on grants
  - Policy influence and citations
  - Public engagement activities
  - Software and technical products
  - Artistic or creative outputs


All enriched records are written incrementally to:

- 📄 `uoe_full_projects_info.jsonl` (158M)  
  → Each line is a standalone JSON object, allowing:
  - Safe streaming
  - Mid-process crash recovery
  - Easy conversion to `.json` or DataFrames later

✅ We advice to run the `fetch_full_info_projects.py`, instead of the notebook for this step, because it takes 5 hours to extract the information.



## ✅ 3. Transforming Extracted GtR Data into the SQL Schema


We align enriched GtR data to our advanced [database schema](https://git.ecdf.ed.ac.uk/impact-tracker/database).

**Key files needed in this part of the information extraction for GtR are**:
- [gtr_uoe_org_info.json](./gtr_uoe_org_info.json) → populates `organization`
- `uoe_full_projects_info.jsonl` → **158MB!**  populates all other tables → Available [in the Google Drive](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link)
- [impact_track_schema_Advanced.sql](https://git.ecdf.ed.ac.uk/impact-tracker/database/-/blob/main/initial_version/impact_track_schema_Advanced.sql?ref_type=heads) → SQL schema

For testing: [test_info.jsonl](./test_info.jsonl)


| SQL Table                     | Extracted From                       | Example Field / Notes                                                                 |
|------------------------------|--------------------------------------|----------------------------------------------------------------------------------------|
| `organization`               | `gtr_uoe_org_info.json` and  `uoe_full_projects_info.jsonl`            | Lead org: *University of Edinburgh*, Funder: *EPSRC* (both added if missing).         |
| `department`                 | `uoe_full_projects_info.jsonl`       | e.g., "Edinburgh Parallel Computing Centre" (manually validated if needed).           |
| `project` + `evidenceitem`   | `uoe_full_projects_info.jsonl`       | Core fields: `grant_ref`, `status`, `funded_value`, `start_date`, `duration`, `title`, and `abstract` (→ `evidence_summary`). Stored as both a `project` and an `evidenceitem`. |
| `publication` + `evidenceitem`  | `uoe_full_projects_info.jsonl`       | Publications with DOIs from enriched outcomes. Stored as `publication` + `evidenceitem`. |
| `software_technical_product`+ `evidenceitem` | `uoe_full_projects_info.jsonl`       | Software entries like `lowFAT`, `SSI-EOG`, `uCONFLY`, etc.                            |
| `dataset` + `evidenceitem`  | `uoe_full_projects_info.jsonl`       | e.g., “International RSE Survey 2022”. Use `doi`, publisher, and description. Also stored as an `evidenceitem`. |
| `policy_influence` + `evidenceitem`  | `uoe_full_projects_info.jsonl`  | Policy contributions listed in outcome sections (e.g., “Turning FAIR into Reality”). Use description, type, URL, and impact. It is also store as `evidenceitem` |
| `activity` + `evidenceitem`  | `uoe_full_projects_info.jsonl`       | Events such as Big Bang Fair, workshops, etc. Include `location`, `event_type`, `event_title`.  Also stored as an `evidenceitem`.|
| `media`    + `evidenceitem`   | `uoe_full_projects_info.jsonl`       | Videos, blog posts, or launch webinars. Use `media_type`, `media_name`, audience. Also stored as an `evidenceitem`. |
| `artistic_creative_product` + `evidenceitem`   | `uoe_full_projects_info.jsonl`       | Map presentations, videos, animations if artistic type clearly indicated. Also stored as an `evidenceitem`. |
| `researcher`                 | `uoe_full_projects_info.jsonl`       | Persons involved with known ORCID (e.g. PI, Co-I). Store full name + ORCID if available. |
| `authored_by`                | `uoe_full_projects_info.jsonl`       | Link `researcher` to `evidenceitem` with `role` (e.g., PI, Co-I) and optionally department. |
| `affiliates`                 | `uoe_full_projects_info.jsonl`       | Populate if department match is known for person.                          |
| `funded_by`                  | `uoe_full_projects_info.jsonl`       | Funders like EPSRC stored as `organization`, linked to project `evidenceitem` via `funded_by`. |
| `project_research_topics`    | `uoe_full_projects_info.jsonl`       | Topical keywords or tags, e.g., "Software Engineering"                                |
| `project_research_subject`   | `uoe_full_projects_info.jsonl`       | Subject domains, e.g., "ICT" or "Energy"                                              |
| `collaboration`              | `uoe_full_projects_info.jsonl`        | Collaborators named in a project; e.g. "Data Carpentry"
| `link`                       | Derived internally                   | Connects `project` to outputs (e.g. software, publications, datasets, policy influence). |


**🧪 Example: The Software Sustainability Institute Project**

```jsonl 
{
  "grantReference": "EP/N006410/1",
  "project_id": "FFB14A0D-1831-4326-9F95-1769239705A3",
  "raw_json_v1": Null,
  "raw_json_v2": Null,
  "sections": [
    {
      "title": "Project Info",
      "fields": {
        "Title": "The Software Sustainability Institute: Phase 2",
        "Grant Reference": "EP/N006410/1",
        "Updated Date": null,
        "Grant Category": "Research Grant",
        "Call": null,
        "Scheme": null,
        "Funder": "EPSRC",
        "Funded Value": "£3,511,601",
        "Start Date": "2015-05-31",
        "End Date": "2019-05-30",
        "Project Duration (months)": 48,
        "Project Cost": null,
        "Actual Spend": null,
        "Status": "Closed",
        "Abstract": "Modern research is impossible without software...",
        "Potential Impact": "The SSI's aim is to impact the research community..."
      }
    },
    {
      "title": "Lead Organisation",
      "fields": {
        "Name": "University of Edinburgh",
        "Department": "Edinburgh Parallel Computing Centre",
        "Postcode": "EH8 9YL",
      }
    },
    {
      "title": "People Involved",
      "people": [
        {
          "name": "Neil Chue Hong",
          "roles": ["PRINCIPAL_INVESTIGATOR"],
          "person_id": "A4C1715E-3FE8-4D0D-AA75-601F45AA46AF",
          "orcid": "0000-0002-8876-7606",
          "affiliated_org_id": "A4735E0B-ED6E-4FC5-9497-CFE564317F54"
        },
        {
          "name": "Carole Goble",
          "roles": ["CO_INVESTIGATOR"],
          "person_id": "719050F9-FD72-4764-8921-1BC52E99512C",
          "orcid" : null
        },
        {
          "name": "Mark Parsons",
          "roles": ["CO_INVESTIGATOR"],
          "person_id": "719050F9-FD72-4764-8921-1BC52E99512C",
          "orcid": "0000-0003-4097-7468"
        } ..... 
      ]
    },
    {
      "title": "Collaborators",
      "collaborators": [
        {
          "id": "318ED98F-1E75-4ED6-9413-994100A7DF13",
          "org_name": "Data Carpentry",
          "type": "N/A",
          "postcode": null
        } ..... 
      ]
    },
    {
      "title": "Research Topics",
      "list": ["Software Engineering"]
    },
    {
      "title": "Research Subjects",
      "list": ["Info. & commun. Technol."]
    },
    {
      "title": "Publications",
      "items": [
        {
          "title": "SSI and Communities of Practice",
          "authors": "Sufi S",
          "doi": "10.5281/zenodo.3941909",
          "publicationUrl": "https://zenodo.org/record/3941909",
          "type": "Other",
          "datePublished": "2020-01-01"
        }, ....
      ]
    },
    {
      "title": "V1 Outcomes",
      "outputs": {
        "policyInfluenceOutputs": [
          {
            "id": "D906785C-1E5B-46C8-AA8E-D43CF2E70D06",
            "outcomeId": "5c8793db361174.74879344",
            "guidelineTitle": null,
            "description": "UNESCO/INRIA Expert Group On Software Source Code As Heritage For Sustainable Development (Neil Chue Hong)",
            "type": "Participation in a guidance/advisory committee",
            "impact": null,
            "areas": [],
            "geographicReach": "Multiple continents/international",
            "url": "https://en.unesco.org/foss/paris-call-software-source-code",
            "methods": null
          },
          ...
        ], 
         "softwareAndTechnicalProductOutputs": [
          {
            "id": "8498C4A3-6321-4B3C-B8DA-EDE978FF5BD6",
            "outcomeId": "5c878f3168a731.72622211",
            "title": "Checklist for a Software Management Plan (source code) (Version 1.0)",
            "description": "A Software Management Plan (SMP) ..",
            "type": "Software",
            "impact": "Checklist implemented in France via OPIDoR: https://dmp.opidor.fr/",
            "url": "https://github.com/softwaresaved/software-management-plans",
            "softwareOpenSourced": null,
            "openSourceLicense": true,
            "providedToOthers": null,
            "yearFirstProvided": 2018
          },
          ...
         ]
        "researchDatabaseAndModelOutputs": [
          ....
        ]
      }
    },
    {
      "title": "V2 Outcomes",
      "outputs": {
        "publications": [
          {
            "links": {
              "link": [
                {
                  "href": "http://gtr.ukri.org/gtr/api/projects/FFB14A0D-1831-4326-9F95-1769239705A3",
                  "rel": "PROJECT",
                  "start": null,
                  "end": null,
                  "otherAttributes": {}
                }
              ]
            },
            "ext": null,
            "id": "B4598461-C1E0-4B37-A07B-A568248393C4",
            "outcomeid": "65e8b88802dd54.21669619",
            "href": "http://gtr.ukri.org/gtr/api//outcomes/publications/B4598461-C1E0-4B37-A07B-A568248393C4",
            "created": 1747301977000,
            ,,,,
          }
          .....
        ]
     }
   }

```

**Resulting Tables**:  

- `organization` → Includes:
  - "University of Edinburgh" (lead institution)
  - "EPSRC" (funder)
  - "Data Carpentry" (collaborator)

- `department` → "Edinburgh Parallel Computing Centre (EPCC)", linked to University of Edinburgh

- `project` → Captures metadata such as:
  - `grant_ref`: "EP/N006410/1"
  - `status`: "Closed"
  - `funded_value`: 3511601
  - `duration`: 48 months
  - `scheme`, `call`, `funder`, `category`, etc.

- `evidenceitem` → Stores:
  - `title`: "The Software Sustainability Institute: Phase 2"
  - `evidence_summary`: abstract and impact text
  - `start_date`, `end_date`

- `researcher` → Includes:
  - "Neil Chue Hong" (with ORCID)
  - "Carole Goble"
  - "Mark Parsons"

- `authored_by` → Links each researcher to the project evidence item with role (PI, CO_INVESTIGATOR)

- `affiliates` → Links PIs (e.g., Neil Chue Hong) to their org/department

- `funded_by` → Connects project to EPSRC (as funder)

- `collaboration` → Connects project to Data Carpentry (as collaborator)

- `project_research_topics` → "Software Engineering"

- `project_research_subject` → "Info. & commun. Technol."

- `publication` → Parsed from `sections.Publications.items`, e.g.:
  - "SSI and Communities of Practice" (with DOI and metadata)

- `software_technical_product` → Parsed from:
  - `softwareAndTechnicalProductOutputs` → (v1 outputs)
  - `softwareandtechnicalproductoutputs` → (v2 outputs)
  - e.g., "Checklist for a Software Management Plan"

- `policy_influence` → Parsed from:
  - `policyInfluenceOutputs` → (v1 outputs)
  - `policyinfluences` → (v2 outputs)
  - e.g., "UNESCO Expert Group on Software as Heritage"

....

- `link` → Used to connect:
  - Project evidenceitem → outputs (publication, software, dataset, policy)


**Important Modelling Decision**: 
- Since we can only obtain (with GtR) the Deparment and Organisation of the PI of a project, we are just creating affliations (researchers connected to deparments) for PIs.
- We are only indication of authorised_by to first authors of publications and policy_influes which are PIs or Collaborators of the grant 

These can be different for other datasets that are not the GtR dataseet. 

###  🛠️ 3.1 Trasformation and Load (TL) Pipeline: (Step 1 and Step 2)

Our **TL pipeline** has two scripts to transform and load the GtR jsonl data into the SQL schema:
- Step 1  → `gtr_to_sqlite_step1.py`: inserts projects as evidenceItems as well as subclasses of evidenceItem (projects), organizations, departments, people, affiliations,   etc 
- Step 2 → `gtr_to_sqlite_step2.py`: inserts outputs as evidenceItems and the corresponding subclasses of evidenceItems (e.g. publications, software) and create links between the each project and the related outputs (also known as evidenceItems)

We additionally have developed bash script, [create_sqlite_gtr.sh](./create_sqlite_gtr.sh), that automates the enteire **TL pipeline** -- See [Section 3.3](https://git.ecdf.ed.ac.uk/impact-tracker/information_extraction/-/blob/main/gtr/gtr_data.md?ref_type=heads#33-automatic-tl-pipeline) 

Right now, our pipeine is configured to transforma and load [test_info.jsonl](./test_info.jsonl). But for transforming and loading the full UoE projects, we need to modify both scripts to use `uoe_full_projects_info.jsonl` instead (available [here](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link)). 

#### 3.1.1  Step 1: Creating Database +  Loading Projects  (without outputs)

The first step of our TL pipeline is to load the [Impact Tracker database schema](https://git.ecdf.ed.ac.uk/impact-tracker/database/-/blob/main/initial_version/impact_track_schema_Advanced.sql?ref_type=heads) into a SQLite database. This creates all the required tables and relations in a new database file (`gtr_impact_tracker.db`):

```shell
sqlite3 gtr_impact_tracker.db < impact_track_schema_Advanced.sql
```

*This command executes the SQL DDL in `impact_track_schema_Advanced.sql`, setting up the empty schema in `gtr_impact_tracker.db`.*
*You need to have a copy of [impact_track_schema_Advanced.sql](https://git.ecdf.ed.ac.uk/impact-tracker/database/-/blob/main/initial_version/impact_track_schema_Advanced.sql?ref_type=heads) into a SQLite database. This creates all the required tables and relations in a new database file (`gtr_impact_tracker.db`):
) in this directory, in order to run the previous command. 


Next, we need to run the [gtr_to_sqlite_step1.py](./gtr_to_sqlite_step1.py) script to populate the database (`gtr_impact_tracker.db`) with project data (without additional ouputs/evidenceItems). The script reads from the **GtR extracted data files** (`test_info.jsonl`  for project details and `gtr_uoe_org_info.json` for UoE organization info) and inserts records into the schema:

```shell
python gtr_to_sqlite_step1.py
```

This script needs:
- [gtr_uoe_org_info.json](./gtr_uoe_org_info.json) → populates `organization`
- project extracted data:
  - `uoe_full_projects_info.jsonl` → **60MB!** Available [in the Google Drive](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link) 
  - OR [test_info.jsonl](./test_info.jsonl)
- `gtr_impact_tracker.db` --> database created in the previous step. 

This step **loads and populates** the following core tables:

- `organization` – e.g. adds **University of Edinburgh** (lead institution) and any **funder organizations** (like *EPSRC*) if they are not already present.
- `department` – inserts departments such as **Edinburgh Parallel Computing Centre (EPCC)**, linked to UoE’s organization ID.
- `project` – for each project, stores key metadata (e.g. `grant_ref`, status, funded_value, duration).
- `evidenceitem` – each project is also stored as an evidence item (with the project **title** and text fields like abstract and potential impact).
- `researcher` – creates entries for **people involved** (Principal Investigators, Co-Investigators, etc., including their names and ORCIDs when available).
- `authored_by` + `affiliates` – links each researcher to the project’s evidence item with their role (e.g. PI, CO_INVESTIGATOR). If a researcher is a PI, the script also records an affiliation in the `affiliates` table tying them to the UoE department.
- `funded_by` – connects each project’s evidence item to its **funder** (inserting the funding organization into `organization` if needed and creating the link).
- `project_research_topics` / `project_research_subject` – adds any research **topic keywords** or broad **subject categories** associated with the project for further classification.

- Additionally, the script skips duplicated projects already present in the database (by checking grantReference).

> Note: we are also capturing people's (PI & collaboratos) and organisations' ids (as gtr_ids), but for other datasets those can be captured (if existed) as om_ids (for opportunity match), or rf_ids (for research fish). 


### Step 3.1.2: Transforming, Loading and Linking Additional Projects Ouputs

The [gtr_to_sqlite_step2.py](./gtr_to_sqlite_step2.py) script continues the GtR transforming and loading process by focusing on **project outputs** (we refer to outputs as evidenceItems as well), such as publications, software, datasets, policy impact, and creative products. It reads from the same **GtR extracted data files** used in step 1 and populates additional tables in the SQL schema, transforming enriched outputs associated with each project:



| Output Type            | SQLite Table                 | Notes                                                                 |
|------------------------|------------------------------|-----------------------------------------------------------------------|
| Publications           | `publication`                | Title, type, URL + authors matched to project researchers            |
| Software & Tools       | `software_technical_product` | Includes open source status, license, first year                     |
| Datasets               | `dataset`                    | Includes type, impact, providedToOthers                              |
| Project Impact Sumary  | `project` & `project_impact_types`| Includes project impact summaries, sector, types, first_years and beneficiaries | 
| Policy Influence       | `policy_influence`           | Cleaned titles; authors extracted from `(Name)` and matched          |
| Artistic / Creative    | `artistic_creative_product`  | Includes type, impact, year                                          |
| Dissemination / Talks  | `activity`                   | Includes form, audience, type, presentationType                      |
| Links                  | `link`                       | All outputs are explicitly linked to the source project              | 


Further notes: 
- F
- Deduplication is applied to avoid duplicate entries (e.g., overlapping V1 and V2 outputs).
- Similarly as the previous script, this one also **skips duplicated projects** already present in the database (by checking `grantReference`).
- Publication authors and policy contributors (from `(...name...)` patterns) are matched to `researcher` entries on the same grant.
- Multiple name variants are supported (e.g., initials, surname-first).


For running the script: 

```bash
python gtr_to_sqlite_step2.py
```

This script needs:
- project extracted data:
  - `uoe_full_projects_info.jsonl` → **60MB!** Available [in the Google Drive](https://drive.google.com/drive/folders/1BRUpduQ6Kt5h5rpjYRPVINJK80FoOVXi?usp=share_link) 
  - OR [test_info.jsonl](./test_info.jsonl)
- `gtr_impact_tracker.db` --> database to populated in the previous step. 

### 3.2  Validate the Data Load

In order to validate the data loaded in our database, we have created two validation scripts (one for each step of the TL pipeline):
- `test_gtr_data_loading_step1.py` -- to **verify** that the data the projects' data (without outputs) have been loaded correctly into all tables
- `test_gtr_data_loading_step2.py`-- to **verify** that the data the projects' outputs (evidenceItems) and links have been loaded correctly into all tables


 ```bash
 python test_gtr_data_loading_step1.py 
```

```
🔍 GtR Database Integrity Test Report:

✅ PASS – evidenceitem has rows (2)
✅ PASS – project has rows (2)
✅ PASS – organization has rows (3)
✅ PASS – department has rows (2)
✅ PASS – researcher has rows (10)
✅ PASS – authored_by has rows (10)
✅ PASS – affiliates has rows (2)
✅ PASS – funded_by has rows (2)
✅ PASS – project_research_topics has rows (2)
✅ PASS – project_research_subject has rows (1)
✅ PASS – Each project has a grant_ref 
✅ PASS – Each evidenceitem has a title 
✅ PASS – Each department has an organization 
✅ PASS – Each authored_by has a researcher and evidence 
```

```bash
python test_gtr_data_loading_step2.py
```
```
✅ Table 'publication' has 168 entries.
✅ Table 'software_technical_product' has 22 entries.
✅ Table 'policy_influence' has 22 entries.
✅ Table 'dataset' has 18 entries.
✅ Table 'artistic_creative_product' has 8 entries.
✅ Table 'media' has 0 entries.
Attention : Table 'media' is empty.
✅ Table 'activity' has 4 entries.
✅ Table 'link' has 242 entries.
🔗 Total project-output links: 242
✅ Step 2 data validation passed.
```

These scripts just need the `gtr_impact_tracker.db` populated before with our **TL pipeline**

### 3.3. Automatic TL Pipeline 

We also provide [create_sqlite_gtr.sh](./create_sqlite_gtr.sh) bash script to automate the full end-to-end **TL Pipeline** for generating the SQLite database:

```bash
bash create_sqlite_gtr.sh
```

This bash script will:
- Remove any previous `gtr_impact_tracker.db` file
- Rebuild the schema from `impact_track_schema_Advanced.sql`
- Run both `gtr_to_sqlite_step1.py` and `gtr_to_sqlite_step2.py`
- Validate data with `test_gtr_data_loading_step1.py` and `test_gtr_data_loading_step2.py`

The output of this TL pipleine is [`gtr_impact_tracker.db`](https://drive.google.com/file/d/1-FOz-sl70qNmhrv36xF_tCG05soqNCTl/view?usp=share_link), ready for downloading it, for running the following steps. 


### 3.4. Inspect a GtR Project from the Database

We have created **four inspection scripts** to interactively inspect a project stored in the [`gtr_impact_tracker.db`](https://drive.google.com/file/d/1-FOz-sl70qNmhrv36xF_tCG05soqNCTl/view?usp=share_link) database by entering its `grantReference` (e.g., `EP/N006410/1`)". 
These scripts just need the `gtr_impact_tracker.db` populated before with our **TL pipeline**. Download it from [here](https://drive.google.com/file/d/1-FOz-sl70qNmhrv36xF_tCG05soqNCTl/view?usp=share_link).  

The first script, [inspect_project.py](./inspect_project.py),  extracts and displays a complete snapshot of the project (without linked outputs, such as publications, activities, etc), including:
  - Project title, description, and duration
  - Lead department and affiliated organization
  - Researchers involved, their roles, and project affiliations
  - Funder(s) and funding details
  - Research topics and subjects
  - Any available affiliations from the `affiliates` table

This is useful for validating the database contents or generating structured summaries for downstream documentation or analysis.

```bash
python inspect_project.py

>> Enter Grant Reference (e.g., EP/N006410/1): EP/N006410/1

🎓 Project: The Software Sustainability Institute: Phase 2 (EP/N006410/1)
------------------------------------------------------------
📄 Description:
Modern research is impossible without software ....
📋 Summary:
None

🏛️ Lead Department: Edinburgh Parallel Computing Centre (University of Edinburgh)

💰 Funded Value: £3,511,601.00
📌 Status: Closed
🔤 Acronym: None
🏷️ Grant Type: Research Grant
📣 Call: None
📘 Scheme: None
⏳ Duration: 48

👩‍🔬 Researchers:
 - Neil Chue Hong (PRINCIPAL_INVESTIGATOR) – Dept: Edinburgh Parallel Computing Centre | 2015-05-31 to 2019-05-30
    📌 PI Affiliation: Edinburgh Parallel Computing Centre, University of Edinburgh | 0000-00-00 to 0000-00-00
 - Stephen Crouch (RESEARCHER) – Dept: N/A | 2015-05-31 to 2019-05-30
 - Carole Goble (CO_INVESTIGATOR) – Dept: N/A | 2015-05-31 to 2019-05-30
 - Mark Parsons (CO_INVESTIGATOR) – Dept: N/A | 2015-05-31 to 2019-05-30
 - Shoaib Sufi (RESEARCHER) – Dept: N/A | 2015-05-31 to 2019-05-30
 - Simon Hettrick (RESEARCHER_COI) – Dept: N/A | 2015-05-31 to 2019-05-30
 - David De Roure (CO_INVESTIGATOR) – Dept: N/A | 2015-05-31 to 2019-05-30
 - Leslie Carr (CO_INVESTIGATOR) – Dept: N/A | 2015-05-31 to 2019-05-30

💰 Funded by:
 - EPSRC

 🏛️ Collaborators:
 - Software Preservation Network
 - The National eResearch Collaboration Tools and Resources project
 - The Carpentries
 - Data Carpentry
 - Software Carpentry Foundation

🏷️ Research Topics:
 - Software Engineering

📚 Research Subjects:
 - Info. & commun. Technol.
```

 To view **all outputs** (**evidenceItems**) associated with a specific project, use [inspect_project_outputs.py](./inspect_project_outputs.py):
```bash
python inspect_project_outputs.py

>> Enter Grant Reference (e.g., EP/N006410/1): EP/N006410/1

📌 Project Title: The Software Sustainability Institute: Phase 2

📝 Impact Summary:
Our findings are being used by industry to understand career paths for Research Software Engineers. This includes the use of data collected as part of our international survey of Research Software Engineers, which has been used in the 2017 Research Software Engineer State of the Nation report, as well as the administrative and logistical support provided to help with the establishment of the UK RSE Association, Society of Research Software Engineering, and similar initiatives in Germany, the Netherlands, Canada and the USA.

The workshop, and subsequent paper, we facilitated on "Four simple recommendations to encourage best practices in research software" has formed the basis of guidance for software development in the life sciences in Europe, and been taken forward by ELIXIR and NLeSC as both a website/campaign (https://softdev4research.github.io/4OSS-lesson/index.html) and course (https://softdev4research.github.io/4OSS-lesson/index.html)

Our Fellowship Programme has also had additional impacts through work that was originally supported through this grant. James Baker's (2015 Fellow) work on Library Carpentry has been incorporated into the international Carpentries curriculum. Robert Haine's (2015 Fellow) and Stephan Druskat's work on tooling to support software citation has been used by GitHub and Zenodo. Caroline Jay (2016 Fellow and subsequently a Co-I on the follow-on grant) has taken forward work on translation to practice.

Work from this grant fed in to the formation of the Research Software Alliance.

🔍 Outputs for Project EP/N006410/1:

[PUBLICATION] The Four Pillars of Research Software Engineering
    ↳ DOI: http://dx.doi.org/10.48550/arxiv.2002.01035
    ↳ Source Link: https://arxiv.org/abs/2002.01035
    ↳ Connection Type: explicit
[PUBLICATION] Software Management Plans as a Solution
    ↳ DOI: http://dx.doi.org/10.5281/zenodo.5013373
    ↳ Source Link: https://zenodo.org/record/5013373
    ↳ Connection Type: explici
...
[POLICY_INFLUENCE] NIH Request for Information on Strategies for NIH Data Management, Sharing, and Citation
    ↳ Connection Type: explicit
[POLICY_INFLUENCE] Open Research Data Task Force
    ↳ Source Link: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/775006/Realising-the-potential-ORDTF-July-2018.pdf
    ↳ Connection Type: explicit

....
[SOFTWARE_TECHNICAL_PRODUCT] CodeMeta Schema Version 1.0
    ↳ Source Link: https://github.com/CodeMeta/codemeta

...
[DATASET] 2016 UK RSE Survey
    ↳ Source Link: https://github.com/softwaresaved/international-survey/blob/master/analysis/2016/uk/data/public_data.csv
    ↳ Connection Type: explicit

...
[ARTISTIC_CREATIVE_PRODUCT] Document all the things! (How again?)
    ↳ Source Link: https://figshare.com/articles/presentation/Document_all_the_things_How_again_/5432788/1
    ↳ Connection Type: explicit
...
[ACTIVITY] Big Bang Fair
    ↳ Event Title: Participation in an activity, workshop or similar
    ↳ Connection Type: explicit
....
👩‍🔬 Researcher Authorship Summary for Publications:
 - Shoaib Sufi: 15 publication(s) [lead_author] on dates: 2018-01-01,2020-01-01,2017-01-01
     ↳ Raising the status of software in research: A survey-based evaluation of the Software Sustainability Institute Fellowship Programme (Preprint)
     ↳ SSI and Communities of Practice (Other)
     ↳ Report on the Workshop on Sustainable Software Sustainability 2019 (WOSSS19) (Other)
     ↳ Raising the status of software in research: A survey-based evaluation of the Software Sustainability Institute Fellowship Programme (Journal Article/Review)
     ↳ Better Software, Better Data Handling (Other)
     ... and 10 more.
 - Carole Goble: 2 publication(s) [lead_author] on dates: 2016-01-01
     ↳ A simple profiling framework for software user-producer maturity review (Conference/Paper/Proceeding/Abstract)
     ↳ Engineering Academic Software (Dagstuhl Perspectives Workshop 16252) (Technical Report)
 ....

👩‍💼 Researcher Authorship Summary for Policy Influences:
 - Carole Goble: 4 policy influence(s) [lead_author] on dates: None
     ↳ UKRI e-Infrastructure Advisory Board [Type: Participation in a guidance/advisory committee; Recognition: National]
     ↳ Open Research Data Task Force [Type: Participation in a guidance/advisory committee; Recognition: National]
     ↳ FAIR in Practice report [Type: Participation in a guidance/advisory committee; Recognition: National]
     ↳ Membershipof ESFRI DIGIT Working Group [Type: Participation in a guidance/advisory committee; Recognition: Europe]
....



🔢 Output Summary:
 - publication: 84
 - policy_influence: 11
 - software_technical_product: 11
 - dataset: 9
 - artistic_creative_product: 4
 - activity: 2

```
This will list the outputs grouped by type (e.g., publications, software, policy). It's ideal for checking enrichment completeness.


## 🔍 3.5 Inspect Researcher-Centric View: `inspect_person_outputs.py`

To complement project-level inspection, we also provide a **researcher-centric inspection script** — [`inspect_person_outputs.py`](./inspect_person_outputs.py) — which summarizes **all projects and outputs linked to a specific researcher** in the GtR-derived database.

### 🔎 What It Does

This script takes a **researcher’s full name** (e.g. `Melissa Terras`) and performs the following:

#### 🧑‍🔬 Project Overview
- Lists all projects where the person is listed in the `authored_by` table.
- Displays for each project:
  - Grant reference and title
  - Funding value and project status
  - Start and End dates of the funding
  - Person’s role (e.g., PI, Co-I, Supervisor)
  - All **Principal Investigators** (can be multiple)
  - Lead department and organization
  - All funded organizations

#### 📦 Linked Outputs (via Projects)
- Aggregates all outputs (`evidenceitem`s) **linked to the projects** the person was involved in:
  - 📚 Publications
  - 💾 Datasets
  - 🛠️ Software & technical products
  - 🏛️ Policy influence items
  - 📰 Media outputs
  - 🎤 Activities
  - 🎭 Artistic/creative products

These outputs are **not required to be authored directly** by the person, but are connected via the `link` table through their associated projects.

#### 📊 Summary Statistics

At the end, the script prints:

- 📁 Total number of projects
- 👩‍🔬 Number of projects where the researcher was **PI**
- 🤝 Number of projects as **Co-I** (i.e., not PI)
- 📝 Total authored **publications**
- 🏛️ Total authored **policy influence** items
- 📦 Count of **all outputs linked to the researcher's projects**, broken down by type

### ▶️ Usage Example

```bash
python inspect_person_outputs.py "Melissa Terras"
```
## 🔍 3.6 Inspect Publication-Centric View: `inspect_publication_output.py`

This [`inspect_publication_output.py`](./inspect_publication_output.py) allows you to inspect **publications stored in the database**, find all partial matches for a given title, and view their metadata alongside the **project(s) they are linked to** via the `link` table.

### 🔎 What It Does

Given a fragment of a publication title, the script:

- Finds all **publications** whose titles contain that substring (case-insensitive)
- For each match, shows:
  - Full publication title
  - Abstract (if available)
  - DOI
  - Number of pages
  - Publication type and review status
  - Lead author (if listed in `authored_by`)
- Shows metadata about the **linked project** (via `link`):
  - Project title
  - Grant reference
  - Principal Investigator (PI)
  - Description preview (first 3 sentences)
  - Start and end dates

### ▶️ Example

```bash
python inspect_publication_output.py dispel4py
```
Produces output like: 
```

🔍 Found 11 publication(s) matching: 'dispel4py'

📄 Title: dispel4py : A Python framework for data-intensive scientific computing
🧾 Abstract: None
🔗 DOI: http://dx.doi.org/10.1177/1094342016649766
📄 Pages: Unknown
📚 Type: Journal Article/Review
✅ Status: published
👤 Lead Author: Unknown
📌 Linked Project: The Terra-correlator: A computing facility for massive real-time data assimilation in environmental science (Grant Ref: NE/L012979/1)
🧑‍🔬 PI: Ian Main
📝 Project Description (Preview): Seismic interferometry: Seismologists have traditionally explored the Earth by measuring, analysing and modelling signals generated naturally by earthquakes... 
📅 Project Dates: 2013-11-14 → 2015-11-13
--------------------------------------------------------------------------------
...
```

## 🔍 3.7 Inspect Policy Influence View: `inspect_policy_output.py`

This script allows you to inspect **policy influence records** stored in the database by performing a **partial title search** and retrieving detailed metadata alongside the **linked project**.

### 🔎 What It Does

Given a fragment of a policy influence title, the script:

- Finds all **policy influence items** whose titles contain the provided string (case-insensitive)
- For each matched record, it prints:
  - Full title
  - Policy type (e.g., advisory role, contribution)
  - Degree of recognition (e.g., national, international)
  - DOI and external source link
  - Full description (if available)
  - Lead author (from `authored_by` table, if available)

It also shows metadata about the **linked project** (via `link`):

- Project title
- Grant reference
- Principal Investigator (PI)
- Start and end dates
- First 3 sentences of project description

### ▶️ Usage Example

```bash
python inspect_policy_output.py "Open Research Data Task Force"
```
Produces output like:
```

🔍 Found 2 policy influence item(s) matching: 'Open Research Data Task Force'

🏛️ Title: FAIRsharing features in the Case Study report by the UK the Open Research Data Task Force.
📘 Type: Influenced training of practitioners or researchers
🎖️ Recognition: National
🔗 DOI: None
🌍 Source Link: https://assets.publishing.service.gov.uk/...
📝 Description: None
👤 Lead Author: Unknown
📌 Linked Project: Delivering ELIXIR-UK (Grant Ref: BB/L005069/2)
🧑‍🔬 PI: Chris Ponting
📝 Project Description (Preview): ELIXIR is a project that is designed to construct and operate a sustainable infrastructure for biological information in Europe...
📅 Project Dates: 2016-01-01 → 2018-05-30
--------------------------------------------------------------------------------

🏛️ Title: Open Research Data Task Force
📘 Type: Participation in a guidance/advisory committee
🎖️ Recognition: National
🔗 DOI: None
🌍 Source Link: https://assets.publishing.service.gov.uk/...
📝 Description: None
👤 Lead Author: Carole Goble
📌 Linked Project: The Software Sustainability Institute: Phase 2 (Grant Ref: EP/N006410/1)
🧑‍🔬 PI: Neil Chue Hong
📝 Project Description (Preview): Modern research is impossible without software From short, thrown-together temporary scripts to solve a specific problem...
📅 Project Dates: 2015-05-31 → 2019-05-30
--------------------------------------------------------------------------------
```
