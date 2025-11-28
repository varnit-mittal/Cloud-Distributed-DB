NAMESPACE=kv-system
K8S_DIR=./k8s

start:
	minikube start --driver=docker --network-plugin=cni --cni=flannel

# Apply all manifests in k8s folder
apply:
	kubectl apply -f $(K8S_DIR)/namespace.yaml
	kubectl apply -R -f $(K8S_DIR)

# Delete everything
delete:
	kubectl delete -f $(K8S_DIR)

# Restart all pods
restart:
	kubectl delete pod -n $(NAMESPACE) --all

# Show all pods in the namespace
pods:
	kubectl get pods -n $(NAMESPACE) -o wide

# Tail logs from controller
logs-controller:
	kubectl logs -n $(NAMESPACE) -l app=controller -f

# Tail logs from a specific worker
# usage: make logs-worker WORKER=worker1
logs-worker:
	kubectl logs -n $(NAMESPACE) -l app=$(WORKER) -f

# Exec into a pod (example: make exec WORKER=worker1)
exec:
	kubectl exec -it -n $(NAMESPACE) $$(kubectl get pod -n $(NAMESPACE) -l app=$(WORKER) -o jsonpath='{.items[0].metadata.name}') -- sh

# Apply ONLY controller changes quickly
controller:
	kubectl apply -f $(K8S_DIR)/controller-deploy.yaml

# Apply ONLY worker deployments quickly
workers:
	kubectl apply -f $(K8S_DIR)/worker1.yaml
	kubectl apply -f $(K8S_DIR)/worker2.yaml
	kubectl apply -f $(K8S_DIR)/worker3.yaml
	kubectl apply -f $(K8S_DIR)/worker4.yaml

# Clean CrashLoopBackOff pods
clean-crash:
	kubectl delete pod -n $(NAMESPACE) $$(kubectl get pods -n $(NAMESPACE) | grep CrashLoopBackOff | awk '{print $$1}')

# Dump k8s events
events:
	kubectl get events -n $(NAMESPACE) --sort-by=.metadata.creationTimestamp
