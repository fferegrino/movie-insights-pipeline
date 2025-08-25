from collections import defaultdict


class LocalMerger:
    def __init__(self):
        self.intervals = defaultdict(list)


    def insert_interval(self, key: str, new_interval: tuple[float, float]):
        new_start, new_end = new_interval
        self.intervals[key].append((new_start, new_end))

    def get_interval(self, key: str) -> list[tuple[float, float]]:
        return self.intervals[key]

def test_local_merger():
    merger = LocalMerger()
    merger.insert_interval("key", (0, 1))
    assert merger.get_interval("key") == [(0, 1)]

    merger.insert_interval("key", (2, 3))
    assert merger.get_interval("key") == [(0, 1), (2, 3)]

    merger.insert_interval("key", (1, 1.5))
    # assert merger.get_interval("key") == [(0, 1.5), (2, 3)]

            