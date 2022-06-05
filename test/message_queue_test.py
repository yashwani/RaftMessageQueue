import pytest

from test_utils import Swarm

TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
PROGRAM_FILE_PATH = "../src/node.py"
ELECTION_TIMEOUT = .30


@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    try:
        node.start(ELECTION_TIMEOUT)
        node.wait_for_startup()
        assert node.put_topic(TEST_TOPIC) == {"success": True}
        yield node
    finally:
        node.clean()


@pytest.fixture
def node():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    try:
        node.start(ELECTION_TIMEOUT)
        node.wait_for_startup()
        yield node
    finally:
        node.clean()


# TOPIC TESTS
def test_get_topic_empty(node):
    assert node.get_topics() == {"success": True, "topics": []}


def test_create_topic(node):
    assert node.put_topic(TEST_TOPIC) == {"success": True}


def test_create_different_topics(node):
    assert node.put_topic(TEST_TOPIC) == {"success": True}
    assert node.put_topic("test_topic_different") == {"success": True}


def test_create_same_topic(node):
    assert node.put_topic(TEST_TOPIC) == {"success": True}
    assert node.put_topic(TEST_TOPIC) == {"success": False}


def test_get_topic(node):
    assert node.put_topic(TEST_TOPIC) == {"success": True}
    assert node.get_topics() == {"success": True, "topics": [TEST_TOPIC]}


def test_get_same_topic(node):
    assert node.put_topic(TEST_TOPIC) == {"success": True}
    assert node.put_topic(TEST_TOPIC) == {"success": False}
    assert node.get_topics() == {"success": True, "topics": [TEST_TOPIC]}


def test_get_multiple_topics(node):
    topics = []
    for i in range(5):
        topic = TEST_TOPIC + str(i)
        assert node.put_topic(topic) == {"success": True}
        topics.append(topic)
    assert node.get_topics() == {"success": True, "topics": topics}


def test_get_multiple_topics_with_duplicates(node):
    topics = []
    for i in range(5):
        topic = TEST_TOPIC + str(i)
        assert node.put_topic(topic) == {"success": True}
        assert node.put_topic(topic) == {"success": False}
        topics.append(topic)
    assert node.get_topics() == {"success": True, "topics": topics}


# MESSAGE TEST


def test_get_message_from_inexistent_topic(node_with_test_topic):
    assert node_with_test_topic.get_message(TEST_TOPIC) == {"success": False}


def test_get_message(node_with_test_topic):
    assert node_with_test_topic.get_message(TEST_TOPIC) == {"success": False}


def test_put_message(node_with_test_topic):
    assert node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE) == {
        "success": True
    }


def test_put_and_get_message(node_with_test_topic):
    assert node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE) == {
        "success": True
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": TEST_MESSAGE,
    }


def test_put2_and_get1_message(node_with_test_topic):
    second_message = TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE) == {
        "success": True
    }
    assert node_with_test_topic.put_message(TEST_TOPIC, second_message) == {
        "success": True
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": TEST_MESSAGE,
    }


def test_put2_and_get2_message(node_with_test_topic):
    second_message = TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE) == {
        "success": True
    }
    assert node_with_test_topic.put_message(TEST_TOPIC, second_message) == {
        "success": True
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": TEST_MESSAGE,
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": second_message,
    }


def test_put2_and_get3_message(node_with_test_topic):
    second_message = TEST_MESSAGE + "2"
    assert node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE) == {
        "success": True
    }
    assert node_with_test_topic.put_message(TEST_TOPIC, second_message) == {
        "success": True
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": TEST_MESSAGE,
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {
        "success": True,
        "message": second_message,
    }
    assert node_with_test_topic.get_message(TEST_TOPIC) == {"success": False}
