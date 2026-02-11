import os 





class PathLayout:
    def __init__(self, base_path: str):
        self.base_path = base_path

    def get_file_path(self, filename: str) -> str:
        return os.path.join(self.base_path, filename)    




class FileStore:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def save(self, data: str):
        with open(self.file_path, 'w') as f:
            f.write(data)

    def load(self) -> str:
        if not os.path.exists(self.file_path):
            return ""
        with open(self.file_path, 'r') as f:
            return f.read()







if __name__ == "__main__":
    file_store = FileStore("data.txt")
    file_store.save("Hello, World!")
    print(file_store.load())
    pass