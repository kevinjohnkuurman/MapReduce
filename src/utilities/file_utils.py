def read_file_contents(file_name, offset=0, length=None):
	try:
		if isinstance(file_name, tuple): f = file_name[0]
		else: f = file_name
		with open(f, "r") as file:
			file.seek(int(offset))
			contents = file.read(length)
			return contents
	except UnicodeDecodeError:
		print(f"Can not read file {file_name}")
	return ""