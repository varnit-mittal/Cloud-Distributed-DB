# operator/operator.py
import kopf
import requests
import os

CONTROLLER = os.environ.get("CONTROLLER_ADDR", "http://controller.kv-system.svc.cluster.local:8000")

@kopf.on.create('kv.example.com', 'v1alpha1', 'keyspacerebalances')
def on_rebalance(spec, name, namespace, logger, **kwargs):
    node = spec.get("node")
    action = spec.get("action", "recover")
    logger.info(f"Rebalance CR created: {name} node={node} action={action}")
    # call controller endpoint to start rebalancing (controller should expose /rebalance)
    try:
        r = requests.post(f"{CONTROLLER}/rebalance", json={"node": node, "action": action}, timeout=10)
        logger.info("Controller response: %s", r.text)
    except Exception as e:
        logger.exception("Failed to request controller rebalance: %s", e)
        raise kopf.TemporaryError("controller call failed", delay=30)
    return {"status": "started"}
