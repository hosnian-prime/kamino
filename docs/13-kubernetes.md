# Kubernetes Integration

## Overview

Kamino runs on Kubernetes as a **StatefulSet** with a **headless Service**. This is the standard pattern for distributed stateful systems — each pod gets a stable DNS name, which is required for consistent node identity and peer discovery.

## Why StatefulSet + Headless Service

A distributed cache needs two things from the platform:

1. **Stable network identity**: Nodes must be able to find each other by address. Deployment pods get random names and IPs on restart — unusable for cluster membership.
2. **Peer discovery**: New pods need to find existing pods to join the cluster. A headless Service provides DNS SRV records listing all pod IPs.

That's it. No PersistentVolume needed (in-memory only), no ordered startup required.

## Discovery Plugin

Kamino uses the Kubernetes Endpoints API to discover peers. This is the standard approach for distributed systems on K8s — the plugin watches the Endpoints resource tied to the headless Service and gets real-time notification when pods come up or go down.

### Why Not Just DNS?

Headless Service DNS returns pod IPs, but:
- DNS has TTL/caching — new pods may not be visible for seconds
- CoreDNS default TTL is 30s — too slow for fast cluster formation
- No push mechanism — requires polling

The Endpoints API provides immediate, event-driven updates.

### Implementation

```rust
pub struct KubernetesDiscovery {
    /// Kubernetes namespace
    namespace: String,
    /// Headless Service name
    service_name: String,
    /// Label selector to match pods
    label_selector: String,
    /// Discovery port
    port: u16,
}

impl DiscoveryPlugin for KubernetesDiscovery {
    fn init(&mut self) -> Result<()> {
        // Uses in-cluster config automatically:
        // - Service account token from /var/run/secrets/kubernetes.io/serviceaccount/token
        // - API server address from KUBERNETES_SERVICE_HOST env var
        // No manual kubeconfig needed
        Ok(())
    }

    fn discover(&self) -> Result<Vec<SocketAddr>> {
        // GET /api/v1/namespaces/{ns}/endpoints/{service}
        // Returns all ready pod IPs from the Endpoints resource
        let endpoints = self.kube_client
            .get_endpoints(&self.namespace, &self.service_name)?;

        let addrs = endpoints.subsets
            .iter()
            .flat_map(|subset| &subset.addresses)  // only "ready" pods
            .map(|addr| SocketAddr::new(addr.ip.parse().unwrap(), self.port))
            .collect();

        Ok(addrs)
    }

    fn register(&self) -> Result<()> {
        // No-op: K8s manages Endpoints automatically via readiness probe
        Ok(())
    }

    fn deregister(&self) -> Result<()> {
        // No-op: K8s removes from Endpoints when pod terminates
        Ok(())
    }
}
```

### How It Fits with SWIM

```
Pod lifecycle:

  Pod starts
    → readiness probe passes
    → K8s adds pod IP to Endpoints
    → discover() returns new pod IP
    → SWIM joins the new pod to cluster
    → routing table rebuilt, data rebalanced

  Pod terminates
    → SIGTERM → graceful leave via SWIM
    → K8s removes pod IP from Endpoints
    → SWIM already knows (leave was broadcast)

  Pod crashes (no graceful leave)
    → K8s removes pod IP from Endpoints
    → SWIM detects via ping timeout → declares dead
    → routing table rebuilt
```

**discover() provides the seed list. SWIM handles the actual up/down detection.** The plugin is called periodically (on join retry, on cluster events) to find new peers that SWIM hasn't seen yet.

### RBAC

The plugin needs minimal read access to Endpoints:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kamino-discovery
rules:
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kamino-discovery
subjects:
  - kind: ServiceAccount
    name: kamino
roleRef:
  kind: Role
  name: kamino-discovery
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kamino
```

Single resource, single verb, namespace-scoped. Minimal surface.

### Configuration

```toml
[discovery]
plugin = "kubernetes"
kubernetes_namespace = "default"       # or read from downward API
kubernetes_service = "kamino"
label_selector = "app=kamino"
bind_port = 3322
```

## Kubernetes Manifests

### Headless Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kamino
spec:
  clusterIP: None          # headless — no load balancing, returns all pod IPs
  selector:
    app: kamino
  ports:
    - name: resp
      port: 3320
      protocol: TCP
    - name: gossip-tcp
      port: 3322
      protocol: TCP
    - name: gossip-udp
      port: 3322
      protocol: UDP        # SWIM uses both TCP and UDP on the same port; both must be declared
```

### StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kamino
spec:
  serviceName: kamino       # binds to headless service
  replicas: 3
  selector:
    matchLabels:
      app: kamino
  template:
    metadata:
      labels:
        app: kamino
    spec:
      serviceAccountName: kamino
      terminationGracePeriodSeconds: 30
      containers:
        - name: kamino
          image: kamino:latest
          ports:
            - containerPort: 3320
              name: resp
              protocol: TCP
            - containerPort: 3322
              name: gossip-tcp
              protocol: TCP
            - containerPort: 3322
              name: gossip-udp
              protocol: UDP
          env:
            - name: KAMINO_DISCOVERY_PLUGIN
              value: "kubernetes"
            - name: KAMINO_KUBERNETES_SERVICE
              value: "kamino.default.svc.cluster.local"
          readinessProbe:
            # CLUSTER.READY returns +OK only when this node has joined the SWIM cluster,
            # received a routing table (signature > 0), and meets member_count_quorum.
            # A TCP-only probe would mark the pod ready while it is still rejecting writes
            # with ErrClusterQuorum — see docs/06-network-protocol.md#cluster-commands.
            exec:
              command:
                - redis-cli
                - -p
                - "3320"
                - CLUSTER.READY
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 3
          livenessProbe:
            # Liveness only checks process responsiveness — a node may be unable to serve
            # writes (quorum lost) but still be alive and recoverable. Don't kill it for that.
            tcpSocket:
              port: 3320
            initialDelaySeconds: 15
            periodSeconds: 10
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "1Gi"
```

## Graceful Shutdown

When Kubernetes sends `SIGTERM` (pod termination, rolling update, scale-down):

```
1. SIGTERM received
2. Kamino begins graceful leave:
   a. Broadcasts leave intention via SWIM gossip
   b. Coordinator recalculates routing table (excludes leaving node)
   c. Balancer migrates owned data to new owners
3. After leave_timeout (default: 5s) or migration complete → process exits
```

`terminationGracePeriodSeconds` in the StatefulSet should be greater than `leave_timeout` to allow migration to complete before `SIGKILL`.

## Probes

| Probe | Mechanism | What it actually verifies |
|-------|-----------|----------------------------|
| Readiness | `exec: redis-cli CLUSTER.READY` | Node has joined SWIM, received a routing table, and meets `member_count_quorum`. If any of these fail, the pod is marked `NotReady` and the headless Service stops returning its IP. |
| Liveness | `tcpSocket: 3320` | Process is alive and the RESP listener is accepting connections. Insufficient as a readiness signal (the listener accepts even when the node would reject writes for quorum reasons), but appropriate for liveness — losing quorum should not kill the process. |

Do **not** use `tcpSocket` for readiness. The original instinct ("if it accepts TCP it's ready") is wrong for any distributed system that has a notion of cluster-level readiness: a freshly started pod accepts TCP within milliseconds but is not actually ready to serve traffic until it has joined SWIM and received a routing table. Misconfiguring this is one of the most common ways to lose traffic during rolling restarts.

## Scaling

### Scale Up
```bash
kubectl scale statefulset kamino --replicas=5
```
New pods start, discover existing peers via headless Service DNS, join via SWIM, coordinator redistributes partitions.

### Scale Down
```bash
kubectl scale statefulset kamino --replicas=3
```
Kubernetes sends SIGTERM to excess pods (highest ordinal first). Each pod gracefully leaves, data migrates to remaining nodes.

## What Kamino Does NOT Need on K8s

| Thing | Why not |
|-------|---------|
| PersistentVolumeClaim | In-memory cache, no disk state |
| Init containers | No ordered startup dependency |
| Pod disruption budget | Optional — nice to have for production, but not a Kamino concern |
| Broad RBAC permissions | Only `get` on `endpoints` in own namespace |
| Sidecar containers | No service mesh dependency |
| ConfigMap for peer list | Headless Service DNS replaces static peer config |
