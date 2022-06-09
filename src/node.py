
import sys

from utils import *
from raftnode import *



def start_external_server(external_port, internal_port, node_id, peer_ports, port_to_id):
    """ Starts the server """
    Raftnode(node_id, external_port, internal_port, peer_ports, port_to_id)
    # Sleep until killed
    while True:
        time.sleep(0.1)


def parse_config():
    """ Parses config file """
    port_to_id = {}  # key = port, value = node_id

    _this_file_name, config_path, node_id = sys.argv
    node_id = int(node_id)

    config_json = json.load(open(config_path))
    node_config = config_json["addresses"][node_id]
    ip, external_port, internal_port = node_config["ip"], node_config["port"], node_config["internal_port"]

    peer_ports = []
    for node_idx in range(len(config_json['addresses'])):
        if node_idx == node_id:
            continue
        other_config = config_json["addresses"][node_idx]
        port_to_id[other_config["internal_port"]] = node_idx
        peer_ports.append(other_config["internal_port"])

    return external_port, internal_port, node_id, peer_ports, port_to_id


if __name__ == "__main__":
    external_port, internal_port, node_id, peer_ports, port_to_id = parse_config()
    start_external_server(external_port, internal_port, node_id, peer_ports, port_to_id)
