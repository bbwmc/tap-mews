# tap-mews

Singer tap for [Mews PMS API](https://mews-systems.gitbook.io/connector-api), built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Streams

| Stream              | Endpoint                        | Primary Key | Replication Key | Parent Stream    |
|---------------------|---------------------------------|-------------|-----------------|------------------|
| services            | /services/getAll                 | Id          | UpdatedUtc      | -                |
| customers           | /customers/getAll                | Id          | UpdatedUtc      | -                |
| reservations        | /reservations/getAll/2023-06-06  | Id          | UpdatedUtc      | -                |
| rates               | /rates/getAll                    | Id          | UpdatedUtc      | -                |
| accounting_categories | /accountingCategories/getAll   | Id          | UpdatedUtc      | -                |
| sources             | /sources/getAll                  | Id          | UpdatedUtc      | -                |
| companies           | /companies/getAll                | Id          | UpdatedUtc      | -                |
| business_segments   | /businessSegments/getAll         | Id          | UpdatedUtc      | -                |
| payment_requests    | /paymentRequests/getAll          | Id          | UpdatedUtc      | -                |
| availability_blocks | /availabilityBlocks/getAll       | Id          | UpdatedUtc      | -                |
| resource_blocks     | /resourceBlocks/getAll           | Id          | UpdatedUtc      | -                |
| rate_groups         | /rateGroups/getAll               | Id          | UpdatedUtc      | services         |
| restrictions        | /restrictions/getAll             | Id          | None            | services         |
| product_service_orders | /productServiceOrders/getAll  | Id          | UpdatedUtc      | services         |
| age_categories      | /ageCategories/getAll            | Id          | UpdatedUtc      | services         |
| resource_categories | /resourceCategories/getAll       | Id          | None            | services         |
| resources           | /resources/getAll                | Id          | UpdatedUtc      | services         |
| products            | /products/getAll                 | Id          | UpdatedUtc      | services         |
| companionships      | /companionships/getAll           | Id          | None            | reservations     |
| order_items         | /orderItems/getAll               | Id          | UpdatedUtc      | reservations     |
| bills               | /bills/getAll                    | Id          | UpdatedUtc      | order_items      |
| payments            | /payments/getAll                 | Id          | UpdatedUtc      | bills            |

**Stream Hierarchy:**
- `services`, `customers`, `reservations`, `rates`, `accounting_categories`, `sources`, `companies`, `business_segments`, `payment_requests`, `availability_blocks`, and `resource_blocks` are independent parent streams
- `resource_categories`, `resources`, `products`, `rate_groups`, `restrictions`, `product_service_orders`, and `age_categories` are children of `services` (partitioned by ServiceId)
- `companionships` and `order_items` are children of `reservations` (order items use reservation IDs as `ServiceOrderIds`)
- `bills` is a child of `order_items`, and `payments` is a child of `bills`
- `reservations`, `customers`, `payment_requests`, `availability_blocks`, and `resource_blocks` use UpdatedUtc time interval filtering (max 3 months)

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
| enterprise_ids | No     |                      | List of Enterprise IDs to scope certain requests (e.g., rates, sources, payment_requests) |
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
