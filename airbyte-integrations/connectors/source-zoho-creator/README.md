# Zoho Creator source connector

This is the repository for the Zoho Creator source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/zoho-creator).

## Local development

### Prerequisites
* Python (~=3.10)
* Poetry (~=1.7) - installation instructions [here](https://python-poetry.org/docs/#installation)


### Installing the connector
From this connector directory, run:
```bash
poetry install --with dev
```


### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/zoho-creator)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_zoho_creator/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.

The connector requires the following configuration:
- `client_id`: The client ID from Zoho API Console
- `client_secret`: The client secret from Zoho API Console
- `client_refresh_token`: Refresh token with scopes `ZohoCreator.report.READ` and `ZohoCreator.meta.application.READ`
- `account_owner_name`: The username of the Zoho Creator account owner
- `app_link_name`: The link name of your Zoho Creator application
- `base_accounts_url`: Base URL for Zoho accounts (e.g., `accounts.zoho.com` or `accounts.zoho.in`)
- `base_url`: Base URL for Zoho Creator API (e.g., `www.zohoapis.com` or `www.zohoapis.in`)


### Locally running the connector
```
poetry run source-zoho-creator spec
poetry run source-zoho-creator check --config secrets/config.json
poetry run source-zoho-creator discover --config secrets/config.json
poetry run source-zoho-creator read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Running unit tests
To run unit tests locally, from the connector directory run:
```
poetry run pytest unit_tests
```

### Building the docker image
1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:
```bash
airbyte-ci connectors --name=source-zoho-creator build --tag=dev
```

An image will be available on your host with the tag `airbyte/source-zoho-creator:dev`.

### Loading the Docker Image in an Airbyte Workspace

If you are developing or testing this connector in an Airbyte workspace (for example, using [Docker Compose](https://docs.airbyte.com/deploying-airbyte/on-docker/)), you'll need to load the Docker image you built locally into the Docker environment used by Airbyte.

#### Using Docker Desktop (Default Local Docker Daemon)

If you are running [Docker Desktop](https://www.docker.com/products/docker-desktop/) and Airbyte via Docker Compose on your local machine, the connector docker image you build locally is automatically usable by your Airbyte instance. Airbyte Docker Compose, running on Docker Desktop, will detect the local image and use it for connector jobs.  
-simply select or specify the `airbyte/source-zoho-creator:dev` image in the Airbyte UI or source configuration. 

> **Note:** When running Airbyte via abctl (which uses kind under the hood), the connector Docker image must be explicitly loaded into the kind cluster. If it isn’t, connector-related operations can fail (including discovery/check) and surface as 5xx errors.  Load it with:  
 
```bash
   kind load docker-image <image-name>:<image-tag> -n airbyte-abctl
```

If you have Airbyte running, you may need to restart its services to ensure it picks up the new connector image.

#### Other Docker Environments (kind, Colima, Remote Docker, etc.)

If Airbyte is running in a different Docker environment (such as `kind`, `colima`, or a remote Docker host), you may need to load or transfer the image into that environment:

```bash
# Save the local image as a tarball
docker save airbyte/source-zoho-creator:dev -o source-zoho-creator-dev.tar

# Load into kind:
kind load image-archive source-zoho-creator-dev.tar --name <kind-cluster-name>

# Load into colima:
colima nerdctl load -i source-zoho-creator-dev.tar
```

Or push it to a remote/container registry and pull from there.

> **Note:** If Airbyte pulls the image from Docker Hub instead of using your local build, confirm that your Airbyte containers are running via Docker Desktop and the image tag exactly matches what you reference in Airbyte's UI/source definition.

For more details, see the [Airbyte docs on local connector development](https://docs.airbyte.com/connector-development/local-development/).

### Running as a docker container
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-zoho-creator:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-zoho-creator:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-zoho-creator:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-zoho-creator:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Running our CI test suite
You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):
```bash
airbyte-ci connectors --name=source-zoho-creator test
```

### Customizing acceptance Tests
Customize `acceptance-test-config.yml` file to configure acceptance tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

### Dependency Management
All of your dependencies should be managed via Poetry. 
To add a new dependency, run:
```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

## Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-zoho-creator test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)): 
    - bump the `dockerImageTag` value in in `metadata.yaml`
    - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/zoho-creator.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.
