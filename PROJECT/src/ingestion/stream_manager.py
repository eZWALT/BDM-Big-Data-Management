# Simple Flask app to manage the ingestion stream producers.

import os
import uuid
from dataclasses import dataclass, field
from multiprocessing import Process
from typing import Any, Dict, List, Literal, Tuple, Type

from flask import Flask, jsonify, request
from loguru import logger

from src.utils.config import ConfigManager

from .stream import KafkaAdmin, ProducerConfig, StreamProducer, _format_topic, _load_stream_producer

app = Flask(__name__)


@dataclass
class Pod:
    client: str
    query: str
    state: Literal["running", "idle", "failed"]
    producer: Type[StreamProducer]
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    init_kwargs: dict = field(default_factory=dict)
    kwargs: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        """
        Convert the Pod instance to a JSON serializable dictionary.
        """
        return {
            "client": self.client,
            "query": self.query,
            "state": self.state,
            "producer": self.producer.__name__,
            "id": self.id,
            "init_kwargs": self.init_kwargs,
            "kwargs": self.kwargs,
        }


def _discover_pods() -> List[Pod]:
    """
    Discover all available pods from the Clients configuration.
    """
    # This is a placeholder implementation. In a real implementation, you would query the Kubernetes API
    # to get the list of pods and their state.
    config = ConfigManager("configuration/stream.yaml")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer_configs: List[ProducerConfig] = config._load_config()["producers"]
    stream_producers: List[Tuple[str, Type[StreamProducer], dict]] = []
    found_names: set[str] = set()

    for producer_config in producer_configs:
        name = producer_config["name"]
        if name in found_names:
            raise ValueError(f"Duplicate producer name: {name}")
        found_names.add(name)
        stream_producers.append(
            (
                name,
                _load_stream_producer(producer_config["py_object"]),
                producer_config.get("kwargs", {}),
            )
        )

    kafka_admin = KafkaAdmin(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    kafka_config = config._load_config()["kafka"]

    products: List[Tuple[str, str]] = []  # This is what should be discovered from the client configs.
    products.append(("nike", "water jordan"))
    products.append(("microsoft", "microsoft excel"))
    products.append(("microsoft", "microsoft incel"))

    pods: List[Pod] = []
    for client, query in products:
        if any(pod.client == client and pod.query == query for pod in pods):
            continue
        for producer_name, producer, kwargs in stream_producers:
            topic = _format_topic(producer_name, query)
            if not kafka_admin.topic_exists(topic):
                kafka_admin.create_topic(
                    topic,
                    num_partitions=kafka_config["num_partitions"],
                    replication_factor=kafka_config["replication_factor"],
                )
            pods.append(
                Pod(
                    client=client,
                    query=query,
                    state="idle",
                    producer=producer,
                    init_kwargs={"topic": topic, "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS},
                    kwargs=kwargs,
                )
            )
    return pods


class PodManager:
    def __init__(self, pods: List[Pod] = None):
        self._pods = pods or []
        self._pod_map = {pod.id: pod for pod in self._pods}
        self._processes: Dict[str, Process] = {}

    @property
    def pods(self) -> List[Pod]:
        """
        Get the list of pods.
        """
        # Update the state of each pod
        for pod in self._pods:
            self._update_pod_state(pod)
        return self._pods

    def get_pod(self, pod_id: str, default=None) -> Pod:
        """
        Get a pod by ID.
        """
        # Check the current state of the pod
        if pod_id not in self._pod_map:
            return default
        pod = self._pod_map[pod_id]
        self._update_pod_state(pod)
        return pod

    def _update_pod_state(self, pod: Pod):
        if pod.id in self._processes:
            process = self._processes[pod.id]
            if process.is_alive():
                if not pod.state == "running":
                    logger.warning(f"Pod {pod.id} state is not running, but process is alive.")
                pod.state = "running"
            else:
                # Check the exit code of the process
                exit_code = process.exitcode
                if exit_code == 0:
                    pod.state = "idle"
                else:
                    pod.state = "failed"
        else:
            pod.state = "idle"

    def add_pod(self, pod: Pod):
        """
        Add a new pod.
        """
        self._pods.append(pod)
        self._pod_map[pod.id] = pod

    def delete_pod(self, pod: Pod):
        self.stop_pod(pod)
        self._pods.remove(pod)
        del self._pod_map[pod.id]

    def stop_pod(self, pod: Pod):
        if not pod.id in self._processes:
            return
        process = self._processes.pop(pod.id)
        process.terminate()
        process.join()
        pod.state = "idle"

    def start_pod(self, pod: Pod):
        pod.state = "running"
        process = Process(
            target=pod.producer(**pod.init_kwargs).produce_forever,
            args=(pod.query,),
            kwargs=pod.kwargs,
            name=pod.id,
        )
        process.start()
        self._processes[pod.id] = process


manager = PodManager(_discover_pods())  # In a production environment, we would use a storage to persist the pods.


@app.get("/pods")
def list_pods():
    """
    List all pods.
    """
    return jsonify([pod.to_json() for pod in manager.pods])


@app.post("/pods")
def create_pod():
    """
    Create a new pod.
    """
    data = request.get_json()
    client = data.get("client")
    query = data.get("query")

    if not client:
        return jsonify({"error": "Client is required"}), 400
    if not isinstance(client, str):
        return jsonify({"error": "Client must be a string"}), 400
    if not query:
        return jsonify({"error": "Query is required"}), 400
    if not isinstance(query, str):
        return jsonify({"error": "Query must be a string"}), 400
    if any(pod.client == client and pod.query == query for pod in manager.pods):
        return jsonify({"error": "Pod with the same client and query already exists"}), 400

    new_pod = Pod(client=client, query=query, state="idle")
    manager.add_pod(new_pod)  # This symbolizes storing it in the management database.
    return jsonify(new_pod.to_json()), 201


@app.get("/pods/<pod_id>")
def get_pod(pod_id):
    """
    Get a specific pod by ID.
    """
    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404
    return jsonify(pod.to_json())


@app.delete("/pods/<pod_id>")
def delete_pod(pod_id):
    """
    Delete a specific pod by ID.
    """
    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404
    manager.delete_pod(pod)
    return jsonify({"message": "Pod deleted"}), 204


@app.put("/pods/<pod_id>")
def update_pod(pod_id):
    """
    Update a specific pod by ID.
    """
    data = request.get_json()

    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404

    new_client = data.get("client")
    new_query = data.get("query")

    if not isinstance(new_client, str):
        return jsonify({"error": "Client must be a string"}), 400
    if not isinstance(new_query, str):
        return jsonify({"error": "Query must be a string"}), 400

    client = new_client or pod.client
    query = new_query or pod.query

    if any(p.client == client and p.query == query and not p.id == pod_id for p in manager.pods):
        return jsonify({"error": "Pod with the same client and query already exists"}), 400

    # Again, this should be persisted in a database.
    pod.client = new_client
    pod.query = new_query

    # Check if the pod is running, and if so, restart it.
    if pod.state == "running":
        manager.stop_pod(pod)
        manager.start_pod(pod)

    return jsonify(pod.to_json())


@app.get("/pods/<pod_id>/state")
def get_pod_state(pod_id: str) -> Any:
    """
    Get the state of a specific pod by ID.
    """
    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404
    return jsonify({"state": pod.state})


@app.post("/pods/<pod_id>/start")
def start_pod(pod_id: str) -> Any:
    """
    Start a specific pod by ID.
    """
    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404

    if pod.state == "running":
        return jsonify({"error": "Pod is already running"}), 400

    manager.start_pod(pod)
    return jsonify({"state": pod.state})


@app.post("/pods/<pod_id>/stop")
def stop_pod(pod_id: str) -> Any:
    """
    Stop a specific pod by ID.
    """
    pod = manager.get_pod(pod_id)
    if not pod:
        return jsonify({"error": "Pod not found"}), 404

    if pod.state != "running":
        return jsonify({"error": "Pod is not running"}), 400

    manager.stop_pod(pod)
    return jsonify({"state": pod.state})


@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint.
    """
    return jsonify({"status": "ok"}), 200
