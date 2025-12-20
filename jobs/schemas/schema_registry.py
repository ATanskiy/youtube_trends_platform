import yaml

class SchemaRegistry:
    def __init__(self, path: str):
        with open(path) as f:
            self.spec = yaml.safe_load(f)

    @property
    def namespace(self):
        return self.spec["namespace"]

    def tables(self):
        return self.spec["tables"].items()

    @staticmethod
    def columns_to_sql(columns):
        return ",\n".join(
            f"{c['name']} {c['type'].upper()}"
            for c in columns
        )