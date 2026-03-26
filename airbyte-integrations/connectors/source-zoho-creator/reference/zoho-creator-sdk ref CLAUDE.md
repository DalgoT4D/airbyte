# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

This project is a sandbox for exploring and testing the `zoho_creator_sdk` Python package.
- Installed version: **0.1.0** (source at `venv/lib/python3.13/site-packages/zoho_creator_sdk/`)
- Latest version: **0.3.1** — reference docs at https://pypi.org/project/zoho-creator-sdk/0.3.1/
- > **Note:** Env var names differ between versions — see the Configuration section below.

## Environment

```bash
# Activate virtual environment (Python 3.13)
source venv/bin/activate

# Run a script
python "zoho-creator-sdk test.py"

# Upgrade to latest version
pip install --upgrade zoho-creator-sdk
```

## Configuration & Auth

`ZohoCreatorClient()` takes no arguments — it auto-loads credentials from env vars or a config file.

### Environment Variables

**v0.3.1 (latest/PyPI docs):**
```bash
export ZOHO_CLIENT_ID="your_client_id"
export ZOHO_CLIENT_SECRET="your_client_secret"
export ZOHO_REFRESH_TOKEN="your_refresh_token"
export ZOHO_CREATOR_DATACENTER="US"          # US, EU, IN, AU, CA, CN, JP
export ZOHO_CREATOR_ENVIRONMENT="development" # optional
export ZOHO_CREATOR_DEMO_USER_NAME="demo"     # optional
```

**v0.1.0 (installed — uses `ZOHO_CREATOR_` prefix for auth vars):**
```bash
export ZOHO_CREATOR_CLIENT_ID="..."
export ZOHO_CREATOR_CLIENT_SECRET="..."
export ZOHO_CREATOR_REDIRECT_URI="..."        # required in v0.1.0
export ZOHO_CREATOR_REFRESH_TOKEN="..."
export ZOHO_CREATOR_DATACENTER="US"           # US, EU, IN, AU, CA
export ZOHO_CREATOR_TIMEOUT=30                # default: 30
export ZOHO_CREATOR_MAX_RETRIES=3             # default: 3
```

### Config File (alternative to env vars)

Searched in order: `./zoho_creator_config.json` → `~/.zoho_creator/config.json` → `/etc/zoho_creator/config.json`

```json
{
  "auth": {
    "client_id": "...",
    "client_secret": "...",
    "redirect_uri": "...",
    "refresh_token": "..."
  },
  "api": {
    "datacenter": "US"
  }
}
```

## SDK Architecture

### Entry Point & Navigation

```python
from zoho_creator_sdk import ZohoCreatorClient

client = ZohoCreatorClient()

# Discover apps for an owner
apps = client.get_applications("your-owner-name")
for app in apps:
    print(f"{app.application_name}  link_name={app.link_name}")

# Style 1: flat — pass all three args directly
form   = client.form("my-app", "owner", "my-form")
report = client.report("my-app", "owner", "all-records")

# Style 2: AppContext — fluent hierarchy (preferred)
app    = client.application(app_link_name="my-app", owner_name="owner")
form   = app.form("my-form")
report = app.report("all-records")
workflow    = app.workflow("my-workflow")
permission  = app.permission("perm-id")
connection  = app.connection("conn-id")
custom_action = app.custom_action("action-name")
```

### Context Classes & Key Methods

| Context | Key Methods |
|---|---|
| `FormContext` | `add_record(data)`, `add_records(records, *, skip_workflow, fields, message, tasks)`, `get_records(criteria=...)` |
| `ReportContext` | `get_records(criteria=..., field_config=..., fields=[...])`, `iter_records_with_cursor(...)`, `get_record(record_id)`, `update_record(record_id, data)`, `delete_record(record_id)` |
| `WorkflowContext` | trigger/manage workflows |
| `PermissionContext` | manage app permissions |
| `ConnectionContext` | manage connections |
| `CustomActionContext` | trigger custom actions |
| `PageContext` / `SectionContext` | metadata access |

### Record Operations

```python
# Fetch all records (auto-paginating generator)
for record in report.get_records():
    print(record.id)
    print(record.get_form_data())   # dict of form fields, excludes metadata

# Fetch with field control
from zoho_creator_sdk.models import FieldConfig

for record in report.get_records(
    field_config=FieldConfig.DETAIL_VIEW,   # QUICK_VIEW | DETAIL_VIEW | CUSTOM | ALL
    fields=["Email", "Phone"],
    criteria='(Status == "Open")',
):
    print(record.get_form_data())

# Cursor-based iteration (v0.3.1+)
for record in report.iter_records_with_cursor(
    field_config=FieldConfig.ALL,
    fields=["Order_ID", "Total"],
):
    process(record)

# Add a single record
response = form.add_record(data={"Name": "Jane", "Status": "Active"})
print(response["data"]["id"])

# Bulk add (max 200 per call)
result = form.add_records(
    records=[{"Name": "A"}, {"Name": "B"}],
    skip_workflow=["form_workflow"],
    fields=["Name"],          # fields to return in response
)

# Update / delete
report.update_record("RECORD_ID", data={"Status": "Closed"})
report.delete_record("RECORD_ID")
```

### Criteria Filtering — Three Formats

```python
# 1. String (manual Zoho Creator syntax)
criteria = '(Name == "John") || (Status == "Active")'

# 2. Dict (structured)
criteria = {
    "Name": "John",                          # equality shorthand
    "Age": {"greater_than": 25},
    "Status": {"in": ["Active", "Pending"]},
    "Email": {"contains": "@company.com"},
}

# 3. CriteriaBuilder (fluent, recommended)
from zoho_creator_sdk.models.criteria import CriteriaBuilder

criteria = (
    CriteriaBuilder()
    .field("Status").equals("Active")
    .and_field("Age").greater_than(18)
    .or_field("Status").equals("Pending")
    .build()
)

for record in form.get_records(criteria=criteria):
    print(record.get_form_data())
```

**All CriteriaBuilder operators:**

| Method | Example |
|---|---|
| `equals(value)` | `.field("Name").equals("John")` |
| `not_equals(value)` | `.field("Status").not_equals("Deleted")` |
| `contains(value)` | `.field("Email").contains("@company.com")` |
| `not_contains(value)` | `.field("Desc").not_contains("spam")` |
| `starts_with(value)` | `.field("Email").starts_with("john@")` |
| `ends_with(value)` | `.field("Email").ends_with("@corp.com")` |
| `greater_than(value)` | `.field("Age").greater_than(18)` |
| `greater_than_or_equal(value)` | `.field("Score").greater_than_or_equal(80)` |
| `less_than(value)` | `.field("Age").less_than(65)` |
| `less_than_or_equal(value)` | `.field("Score").less_than_or_equal(100)` |
| `between(start, end)` | `.field("Date").between("2024-01-01", "2024-12-31")` |
| `in_list(values)` | `.field("Status").in_list(["Active", "Pending"])` |
| `not_in_list(values)` | `.field("Status").not_in_list(["Deleted"])` |
| `is_empty()` | `.field("Notes").is_empty()` |
| `is_not_empty()` | `.field("Notes").is_not_empty()` |

Chain with `.and_field(name)` or `.or_field(name)`, end with `.build()`.

### Models

All models are **Pydantic**-based. `Record` uses `extra="allow"` to accept any form fields.

Key models: `Application`, `Record`, `User`, `Workflow`, `Permission`, `Connection`, `CustomAction`, `Page`, `Section`, `Report`, `ReportColumn`, `ReportFilter`, `FormSchema`, `FormField`, `BulkOperation`, `ImportResult`, `ExportResult`

### Exception Hierarchy

```
ZohoCreatorError
├── AuthenticationError
│   ├── TokenExpiredError
│   ├── TokenRefreshError      # has .is_recoverable attribute
│   └── InvalidCredentialsError
├── APIError                   # has .status_code attribute
│   ├── RateLimitError
│   ├── ResourceNotFoundError
│   ├── BadRequestError
│   ├── ServerError
│   ├── ZohoPermissionError
│   ├── ZohoTimeoutError
│   └── QuotaExceededError
├── ConfigurationError
└── NetworkError
```

### Key Enums

- `Datacenter`: `US`, `EU`, `IN`, `AU`, `CA` (+ `CN`, `JP` in v0.3.1)
- `FieldConfig`: `QUICK_VIEW`, `DETAIL_VIEW`, `CUSTOM`, `ALL`
- `FieldType`: `TEXT`, `NUMBER`, `EMAIL`, `DATE`, `DATETIME`, `DROPDOWN`, `CHECKBOX`, `LOOKUP`, `SUBFORM`, `FORMULA`, `FILEUPLOAD`, etc.
- `ImportFormat` / `ExportFormat`: `CSV`, `JSON`, `XLSX`, `XLS`, `PDF`, `TSV`, `XML`, `YAML`
- `ImportMode`: `CREATE`, `UPDATE`, `UPSERT`, `DELETE`, `VALIDATE`
- `WorkflowType`: `APPROVAL`, `EMAIL`, `FIELD_UPDATE`, `WEBHOOK`, `SCHEDULED`, etc.
- `TriggerType`: `RECORD_CREATED`, `RECORD_UPDATED`, `FIELD_CHANGED`, `SCHEDULED`, `MANUAL`, etc.
- `ActionType`: `EMAIL_NOTIFICATION`, `FIELD_UPDATE`, `WEBHOOK_CALL`, `APPROVAL_REQUEST`, etc.
- `PermissionType`: `READ`, `WRITE`, `DELETE`, `CREATE`, `ADMIN`, `EXPORT`, `IMPORT`, etc.
- `BulkOperationStatus`: `PENDING`, `IN_PROGRESS`, `COMPLETED`, `FAILED`, `CANCELLED`

## Dependencies

- `httpx` — HTTP client (with automatic retries and exponential backoff)
- `pydantic` — data models and validation
