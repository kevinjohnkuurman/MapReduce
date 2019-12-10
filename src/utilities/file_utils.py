def read_file_contents(file_name):
    try:
        with open(file_name, "r") as file:
            contents = file.read()
        return contents
    except UnicodeDecodeError:
        print(f"Can not read file {file_name}")
    return ""