import uuid
import os

class CryptoHelper:

    def __init__(self, partition_date: int, partition_hour: int):
        self.partition_date = partition_date
        self.partition_hour = partition_hour
        self.keys = []

        if not os.path.exists(f"secrets/keys"):
            os.makedirs(f"secrets/keys")
        
    def read_keys(self):
        
        if self.keys is []:
            if os.path.exists(f"secrets/keys/key_{self.partition_date}_{self.partition_hour}.txt"):
                with open(f"secrets/keys/key_{self.partition_date}_{self.partition_hour}.txt", "r") as f:
                    for line in f:
                        self.keys.append(int(line.strip()))

    def generate_rendom_key(self):
        return int(uuid.uuid4()) % 26

    def save_key(self, key):
        with open(f"secrets/keys/key_{self.partition_date}_{self.partition_hour}.txt", "a") as f:
            f.write(str(key) + "\n")
        self.keys.append(key)

    def encrypt(self, value: str):
        key = self.generate_rendom_key()
        value = self.ecrypt_text(value, key)
        self.save_key(key)
        return value

    def ecrypt_text(self, plain_text: str, key: int):
        chiper_text = ""
        for ch in plain_text:
            chiper_text += chr(ord(ch) + key)
        return chiper_text
    
    def decrypt(self, chiper_text: str, index: int):
        key = self.get_key()
        plain_text = ""
        for ch in chiper_text:
            plain_text += chr(ord(ch) - key)
        return plain_text
    
    def get_key(self, index: int):
        return self.keys[index]


