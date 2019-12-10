from types import FunctionType
from utilities.file_utils import read_file_contents

class Script:
    """
    This is an utility to convert code contained in a string into an
    executable python object
    """
    def __init__(self, code: str):
        try:
            self.module = {
                'read_file': read_file_contents,
            }
            exec(compile(code, '<MapReduce>', 'exec'), self.module)
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
        return self.module.get(name, None)