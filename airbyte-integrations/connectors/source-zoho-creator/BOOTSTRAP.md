The Zoho Creator connector enables you to sync data from your Zoho Creator applications to any supported data warehouse.

## Overview

This source connector is built on the Airbyte CDK. It uses the Zoho Creator Data API v2.1 to discover available reports within a specified application, then retrieves records from those reports.

## Prerequisites

To use the Zoho Creator source connector, you need:
- A Zoho Creator account with applications containing reports
- Zoho API authentication credentials:
  - **Client ID** and **Client Secret** from Zoho API Console
  - **Refresh Token** with required scopes: `ZohoCreator.report.READ` and `ZohoCreator.meta.application.READ`
- Your Zoho account's **Account Owner Username** and **App Link Name** (found in your Zoho Creator application URL)
- Your data center's **Base Accounts URL** and **Base API URL** (e.g., `accounts.zoho.com` and `www.zohoapis.com` for US data center)

## Getting Started

### Setting up Zoho Creator API Credentials

1. Log in to your Zoho account and navigate to [Zoho API Console](https://api-console.zoho.com/)
2. Create an OAuth application to obtain your Client ID and Client Secret
3. Generate a Refresh Token by authorizing the application with the required scopes:
   - `ZohoCreator.report.READ`
   - `ZohoCreator.meta.application.READ`
4. Identify your data center region and note the corresponding URLs:
   - **US**: `accounts.zoho.com` / `www.zohoapis.com`
   - **EU**: `accounts.zoho.eu` / `www.zohoapis.eu`
   - **AU**: `accounts.zoho.com.au` / `www.zohoapis.com.au`
   - **IN**: `accounts.zoho.in` / `www.zohoapis.in`
   - **CN**: `accounts.zoho.com.cn` / `www.zohoapis.com.cn`
   - **JP**: `accounts.zoho.jp` / `www.zohoapis.jp`

### Using the Airbyte Connector

1. In Airbyte, create a new source and select "Zoho Creator"
2. Enter your configuration:
   - **Client ID**: From Zoho API Console
   - **Client Secret**: From Zoho API Console
   - **Refresh Token**: With required scopes
   - **Account Owner Username**: Your Zoho Creator account owner username
   - **App Link Name**: The link name of your target Zoho Creator application
   - **Base Accounts URL**: Your Zoho accounts base URL (e.g., `accounts.zoho.com`)
   - **Base API URL**: Your Zoho Creator API base URL (e.g., `www.zohoapis.com`)
3. Click "Test Connection" to verify your credentials
4. Proceed with discovering available reports and configuring the sync

## Supported Operations

- **Full Refresh**: Complete reload of all records from each report
- **Incremental Sync**: Automatically enabled for reports that contain `Added_Time` or `Modified_Time` fields. The connector tracks the most recent timestamp and only syncs new or modified records in subsequent runs

## Data Types and Schema

The connector automatically discovers the schema for each report by analyzing sample data. All scalar values from the Zoho Creator API are treated as strings. Fields containing objects or arrays are handled accordingly.

## Rate Limiting

Zoho Creator API has rate limits. The connector respects these limits with automatic backoff and retry logic built into the Airbyte CDK.

## Known Limitations

- Schema inference is based on sample data (first 20 records), so fields present only in later records may not be captured
- All scalar field values are typed as strings, as the Zoho Creator Data API returns all fields as strings
- Reports without `Added_Time` or `Modified_Time` fields will only support full refresh syncs
- Very large reports may take longer to sync due to API pagination

## Troubleshooting

- Verify your API credentials are correct and that your refresh token includes the required scopes
- Ensure your account owner name and app link name match exactly (case-sensitive)
- Check that your base URLs correspond to your Zoho data center region
- Verify that your refresh token hasn't expired
- Review the Airbyte logs for detailed error messages

For more information, see the [Zoho Creator API Documentation](https://www.zoho.com/creator/api/).
