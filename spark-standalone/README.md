# Spark Standalone Helm Chart

Apache Spark Standalone is a cluster manager that provides an alternative to using Kubernetes Operator or YARN for managing Spark resources. This deployment provides a dedicated Spark cluster within Kubernetes.

## Prerequisites

- Kubernetes 1.20+
- Kubernetes Ingress controller
- Storage provisioner for PersistentVolumes (optional)

## Installation

```bash
# Deploy the Spark master
kubectl apply -f spark-master-deployment.yaml
kubectl apply -f spark-master-service.yaml
kubectl apply -f spark-maskter-ingress.yaml

# Deploy the Spark workers
kubectl apply -f spark-worker-deployment.yaml
kubectl apply -f spark-workers-svc.yaml
kubectl apply -f spark-worker-routes.yaml
```

### ArgoCD Integration

Spark Standalone can be deployed via ArgoCD using the application manifest:

```bash
kubectl apply -f argocd-apps/spark-standalone.yaml
```

## Architecture

This deployment consists of multiple Kubernetes resources:

1. **Spark Master**: Central coordinator for the Spark cluster
   - Deployment: `spark-master-deployment.yaml`
   - Service: `spark-master-service.yaml`
   - Ingress: `spark-maskter-ingress.yaml`

2. **Spark Workers**: Nodes that execute Spark tasks
   - Deployment: `spark-worker-deployment.yaml`
   - Service: `spark-workers-svc.yaml`
   - Routes: `spark-worker-routes.yaml`

## Client Connection

The deployment includes an improved Spark client script (`improved-spark-client.py`) for connecting to the Spark cluster from within the Kubernetes environment.

## Integration with Data Platform

Spark Standalone integrates with other components in the data platform:

- **JupyterHub**: For interactive Spark development
- **Airflow**: For orchestrating Spark jobs
- **Lakekeeper**: For data governance

## Scaling

To scale the Spark cluster:

1. Modify the `replicas` field in `spark-worker-deployment.yaml`
2. Apply the changes using kubectl or ArgoCD

## Monitoring

The Spark UI is exposed through the master ingress for monitoring job progress and cluster resources.

## Troubleshooting

- **Connection issues**: Verify services and ingress are correctly configured
- **Worker registration failures**: Check worker logs for connection problems with the master
- **Resource constraints**: Monitor worker pod resource usage

## Maintenance

For updates:
1. Update the Spark image version in the deployment files
2. Apply the changes using kubectl or ArgoCD
3. Verify master and workers are reconnected correctly
