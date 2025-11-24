# tap-mews

Singer tap for [Mews PMS API](https://mews-systems.gitbook.io/connector-api), built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Streams

| Stream              | Endpoint                   | Primary Key | Replication Key |
|---------------------|----------------------------|-------------|-----------------|
| resource_categories | /resourceCategories/getAll | Id          | UpdatedUtc      |
| resources           | /resources/getAll          | Id          | UpdatedUtc      |
| reservations        | /reservations/getAll       | Id          | UpdatedUtc      |
| customers           | /customers/getAll          | Id          | UpdatedUtc      |
| services            | /services/getAll           | Id          | UpdatedUtc      |

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

| Setting      | Required | Default                 |
|--------------|----------|-------------------------|
| client_token | Yes      |                         |
| access_token | Yes      |                         |
| api_url      | No       | https://api.mews.com    |
| client_name  | No       | BBGMeltano 1.0.0        |

```bash
meltano config tap-mews set client_token YOUR_CLIENT_TOKEN
meltano config tap-mews set access_token YOUR_ACCESS_TOKEN
```

## Usage

```bash
meltano invoke tap-mews --discover
meltano run tap-mews target-postgres
```
