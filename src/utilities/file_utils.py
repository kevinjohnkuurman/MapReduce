def read_file_contents(file_name):
    try:
        with open(file_name, "r") as file:
            contents = file.read()
        return contents
    except:
        print(f"Can not read file {file_name}")
    return ""

def read_file_chunk(file_name, offset, length):
    try:
        with open(file_name, "r") as file:
            file.seek(offset)
            contents = file.read(length)
        return contents
    except:
        print(f"Can not read file {file_name}")
    return ""

