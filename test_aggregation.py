import unittest

from process_and_transform import aggregation

def test_dropoff():
    assert aggregation('2024-01-28T10:20:40.7316-05:00','dropoff',4) == {'2024-01-28T10': (0, 1, 4)}, 'test_dropoff failed'

def test_pickup():
    assert aggregation('2024-01-28T10:20:40.7316-05:00','pickup',4) == {'2024-01-28T10': (1, 1, 8)}, 'test_pickup failed'

def test_new_hour():
    assert aggregation('2024-01-28T11:20:40.7316-05:00','dropoff',3) == {'2024-01-28T10': (1, 1, 8),'2024-01-28T11': (0, 1, 3)}, 'test_new_hour failed'

if __name__ == "__main__":
    try:
        test_dropoff()
        test_pickup()
        test_new_hour()
        print("Everything passed")
    except AssertionError as msg: 
        print(msg)


