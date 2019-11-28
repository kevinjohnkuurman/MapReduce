from types import ModuleType, FunctionType


class Script:
    """
    This is an utility to convert code contained in a string into an
    executable python object
    """
    def __init__(self, code: str):
        try:
            self.module = ModuleType("customModule")
            exec(code, self.module.__dict__)
        except SyntaxError:
            print("Could not compile script")
            raise RuntimeError("invalid script")

    def has(self, name: str) -> bool:
        """
        :param name: the function name to query
        :return: True if
        """
        return self.get(name) is not None

    def get(self, name) -> FunctionType:
        """
        :param name: the function name to query
        :return: a function if available, None otherwise
        """
        return self.module.__dict__.get(name, None)