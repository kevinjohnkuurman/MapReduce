import threading

class AtomicInteger:
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def inc_get(self):
        with self._lock:
            self._value += 1
            return self._value

    def get_inc(self):
        with self._lock:
            self._value += 1
            return self._value - 1

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v
            return self._value