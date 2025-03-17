import json


class JsonParse:
    def __init__(self, tokens_file_manager):
        self.json_data: dict = {}
        self.tokens_file_manager = tokens_file_manager

    def parse(self) -> dict:
        file_path = self.tokens_file_manager.get_tokens_file_path()
        with open(file_path, "r") as f:
            data_from_json = json.load(f)
            for key, value in data_from_json.items():
                self.json_data[key] = {
                    'contract_address': value['contract_address'],
                    'chain': value['chain'],
                    'minimum_spread': value.get('minimum_spread', 6.0)
                }
        return self.json_data

if __name__ == "__main__":
    j = JsonParse()
    j.parse()
