# tap-mews

Singer tap for [Mews PMS API](https://mews-systems.gitbook.io/connector-api), built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Streams

| Stream              | Endpoint                   | Primary Key | Replication Key | Parent Stream |
|---------------------|----------------------------|-------------|-----------------|---------------|
| services            | /services/getAll           | Id          | UpdatedUtc      | -             |
| customers           | /customers/getAll          | Id          | UpdatedUtc      | -             |
| resource_categories | /resourceCategories/getAll | Id          | None            | services      |
| resources           | /resources/getAll          | Id          | UpdatedUtc      | services      |
| reservations        | /reservations/getAll       | Id          | UpdatedUtc      | services      |

**Stream Hierarchy:**
- `services` and `customers` are independent parent streams
- `resource_categories`, `resources`, and `reservations` are children of `services` (partitioned by ServiceId)
- Both `reservations` and `customers` use UpdatedUtc time interval filtering (max 3 months)

## Installation

```bash
meltano add extractor tap-mews --custom
# pip_url: git+https://github.com/bbwmc/tap-mews.git
# executable: tap-mews
```

Or add to `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-mews
      namespace: tap_mews
      pip_url: git+https://github.com/bbwmc/tap-mews.git
      executable: tap-mews
```

## Configuration

| Setting      | Required | Default              | Description                                                                  |
|--------------|----------|----------------------|------------------------------------------------------------------------------|
| client_token | Yes      |                      | Mews API Client Token                                                        |
| access_token | Yes      |                      | Mews API Access Token                                                        |
| start_date   | Yes      |                      | Start date for retrieving reservations (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ) |
| api_url      | No       | https://api.mews.com | Mews API base URL                                                            |
| client_name  | No       | BBGMeltano 1.0.0     | Client identifier sent with API requests                                     |

**Note:** The Mews API has a 3-month maximum for date ranges. If your start_date is more than 3 months in the past, the tap will automatically cap the range and log a warning. Use incremental syncs with state to continue from where you left off.

```bash
meltano config tap-mews set client_token YOUR_CLIENT_TOKEN
meltano config tap-mews set access_token YOUR_ACCESS_TOKEN
meltano config tap-mews set start_date 2024-01-01
```

## Usage

```bash
meltano invoke tap-mews --discover
meltano run tap-mews target-postgres
```
