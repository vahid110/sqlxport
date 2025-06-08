class QueryEngine:
    def preview(self, file_path: str, limit: int = 10) -> str:
        raise NotImplementedError

    def infer_schema(self, file_path: str):
        raise NotImplementedError

    def validate_table(self, table_name: str, **kwargs):
        raise NotImplementedError
