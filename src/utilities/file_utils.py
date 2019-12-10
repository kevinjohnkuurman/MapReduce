def read_file_contents(file_name):
    with open(file_name, "r") as file:
        contents = file.read()
    return contents