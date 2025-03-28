from datetime import datetime, timezone

class DataUpdated:
    """
    Evento de domínio disparado após a atualização dos dados.
    """
    def __init__(self, entity, timestamp=None):
        self.entity = entity
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def __repr__(self):
        return f"<DataUpdated entity={self.entity} timestamp={self.timestamp}>"
