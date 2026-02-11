import os



class DatabaseConfig:  
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL', 'sqlite:///./test.db')
        