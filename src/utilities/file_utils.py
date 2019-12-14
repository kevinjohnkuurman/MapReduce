def read_file_contents(file_name, offset=0, length=None):
	try:
		with open(file_name, "r") as file:
			file.seek(int(offset))
			contents = file.read(length)
			return contents
	except UnicodeDecodeError:
		print(f"Can not read file {file_name}")
	return ""