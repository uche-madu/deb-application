
# 🗜️⚙️ DEB Application Repository 🔩🧰
![Airflow Image Build](https://github.com/uche-madu/deb-application/actions/workflows/retag.yaml/badge.svg)

***This is one of two repositories with code for the entire DEB Project. While this repository focuses on the application code such as Airflow DAGs, the [DEB Infrastructure repository](https://github.com/uche-madu/deb-infrastructure) focuses on provisioning cloud resources. This separation of concerns via separate repositories aims to follow GitOps Principles.***

## CI/CD Workflow
The `build-push.yaml` Github Actions workflow builds a custom airflow image and pushes it to Google Artifact Registry. It then checks out the [DEB Infrastructure repository](https://github.com/uche-madu/deb-infrastructure) to update the `airflow-helm/values-dev.yaml` file with the new image and tag. For this to work, create a repository-scoped `Personal Access Token` for your Github account and store it as a secret in this repository. Visit [Github Docs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic) for details about creating tokens.