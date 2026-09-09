# Spark Operator Helm Chart

The Kubernetes Operator for Apache Spark aims to make specifying and running Spark applications as easy and idiomatic as running other workloads on Kubernetes.

## Prerequisites

- Kubernetes 1.20+
- Helm 3.0+
- Storage provisioner for PersistentVolumes (optional)

## Security Context Constraints

No specific SCC is required for Spark Operator itself, but the spawned Spark pods may require specific permissions depending on your workloads.

## Installation

```bash
# Deploy using custom values
helm install spark-operator -f spark-tweaked-values.yml .
```

### ArgoCD Integration

Spark Operator can be deployed via ArgoCD using the application manifest:

```bash
kubectl apply -f argocd-apps/spark-operator-app.yaml
```

## Architecture

This Helm chart deploys the following components:

1. **Spark Operator**: The main controller that manages SparkApplication resources
2. **Webhook Server**: For validating and mutating Spark application configurations
3. **Custom Resource Definitions**: For SparkApplication and ScheduledSparkApplication

## Custom Spark Images

This deployment includes a custom Spark image with additional libraries and configurations:
- Located in the `spark-custom-image` directory
- Pre-installed with common data science libraries
- Configured for S3/cloud storage access

## Resource Specifications

The deployment includes node resource definitions in `node_resources.tsv` to help allocate appropriate resources for Spark applications.

## Spark Applications

Sample Spark application definitions:
- `sparkapp.yml`: Template for creating new Spark applications

## Integration with Data Platform

Spark Operator integrates with other components in the data platform:

- **Jupyter**: For interactive Spark development
- **Airflow**: For orchestrating Spark jobs
- **S3/Cloud Storage**: For data access
- **Lakekeeper**: For data governance

## Customization

The deployment uses a customized values file:
- `spark-tweaked-values.yml`: Contains environment-specific adjustments
- `sia-team-values.yaml`: Team-specific configurations

## Troubleshooting

- **Application failures**: Check SparkApplication events and logs
- **Resource constraints**: Verify node capacity and resource requests/limits
- **Driver/Executor issues**: Check pod logs for configuration problems

## Maintenance

Regular updates should be performed by updating the Chart version and applying the changes through ArgoCD.

For updating the custom Spark image:
1. Modify the Dockerfile in `spark-custom-image`
2. Build and push the new image
3. Update the image reference in the values file
