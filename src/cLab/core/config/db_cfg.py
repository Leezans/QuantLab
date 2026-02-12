import os

if os.name == 'nt':  # Windows
    CRYPTOSDATABASEPATH = "G:/database/crypto/"
elif os.name == 'posix':  # macOS or Linux
    CRYPTOSDATABASEPATH = "/Users/borealis/Documents/database/crypto"


class FileDatabase:
    def __init__(self, file_path=CRYPTOSDATABASEPATH):
        self.databasePath = file_path






if __name__ == "__main__":  
    print(CRYPTOSDATABASEPATH)



    pass