import pytest

from test_utils import Swarm, LEADER

# seconds the program will wait after starting a node for election to happen
# it is set conservatively, you will likely be able to lower it for faster tessting
ELECTION_TIMEOUT = 2.0

# array of numbr of nodes spawned on tests, an example could be [3,5,7,11,...]
# default is only 5 for faster tests
NUM_NODES_ARRAY = [5]

# your `node.py` file path
PROGRAM_FILE_PATH = "../src2/node.py"


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    try:
        swarm.start(ELECTION_TIMEOUT)
        yield swarm
    finally:
        swarm.clean()


@pytest.mark.parametrize("num_nodes", [1])
def test_correct_status_message(swarm: Swarm, num_nodes: int):
    status = swarm[0].get_status()
    assert "role" in status.keys()
    assert "term" in status.keys()
    assert type(status["role"]) == str
    assert type(status["term"]) == int