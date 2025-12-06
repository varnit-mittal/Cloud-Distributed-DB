NAMESPACE=kv-system
K8S_DIR=./k8s

start:
	minikube start --driver=docker --network-plugin=cni --cni=flannel

apply:
	kubectl apply -f $(K8S_DIR)/namespace.yaml
	kubectl apply -R -f $(K8S_DIR)

delete:
	kubectl delete -f $(K8S_DIR)

restart:
	kubectl delete pod -n $(NAMESPACE) --all

pods:
	kubectl get pods -n $(NAMESPACE) -o wide

logs-controller:
	kubectl logs -n $(NAMESPACE) -l app=controller -f

logs-worker:
	kubectl logs -n $(NAMESPACE) -l app=$(WORKER) -f

exec:
	kubectl exec -it -n $(NAMESPACE) $$(kubectl get pod -n $(NAMESPACE) -l app=$(WORKER) -o jsonpath='{.items[0].metadata.name}') -- sh

controller:
	kubectl apply -f $(K8S_DIR)/controller-deploy.yaml

workers:
	kubectl apply -f $(K8S_DIR)/worker1.yaml
	kubectl apply -f $(K8S_DIR)/worker2.yaml
	kubectl apply -f $(K8S_DIR)/worker3.yaml
	kubectl apply -f $(K8S_DIR)/worker4.yaml

clean-crash:
	kubectl delete pod -n $(NAMESPACE) $$(kubectl get pods -n $(NAMESPACE) | grep CrashLoopBackOff | awk '{print $$1}')

events:
	kubectl get events -n $(NAMESPACE) --sort-by=.metadata.creationTimestamp
