The Zoho Creator connector enables you to sync data from your Zoho Creator applications to any supported data warehouse.

## Overview

This source connector is built on the Airbyte CDK. It uses the Zoho Creator Data API to discover available applications and forms, then retrieves records from those forms.

## Pre-requisites

To use the Zoho Creator source connector, you need:
- A Zoho Creator account with applications containing forms/tables
- Zoho API authentication credentials (Client ID, Client Secret, and Refresh Token)

## Getting Started

### Setting up Zoho Creator

1. Log in to your Zoho account and navigate to Zoho API Console
2. Create an OAuth application to get your Client ID and Client Secret
3. Generate a Refresh Token by authorizing the application
4. Note your data center region (US, EU, AU, IN, CN, or JP)

### Using the Airbyte Connector

1. In Airbyte, create a new source and select "Zoho Creator"
2. Enter your Client ID, Client Secret, and Refresh Token
3. Select your data center region
4. (Optional) Set a start date for incremental syncs
5. Click "Test Connection" to verify credentials
6. Proceed with discovering available forms and configuring the sync

## Supported Operations

- **Full Refresh**: Complete reload of all records from each form
- **Incremental**: Load only new/modified records since the last sync

## Rate Limiting

Zoho Creator API has rate limits. The connector respects these limits with automatic backoff and retry logic.

## Data Types

Records from Zoho Creator forms are loaded with all available fields. The schema is automatically discovered from the form definition.

## Known Limitations

- Attachments and relationships may require special handling
- Very large forms may take longer to sync
- Some custom field types may not be fully supported yet

## Troubleshooting

- Verify your API credentials are correct
- Ensure your Zoho account has the required API access
- Check that your refresh token hasn't expired
- Review the Airbyte logs for detailed error messages

For more information, see the [Zoho Creator API Documentation](https://www.zoho.com/creator/api/).
