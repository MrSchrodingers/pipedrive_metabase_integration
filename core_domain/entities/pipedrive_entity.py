class PipedriveEntity:
    """
    Entidade que representa os dados do Pipedrive.
    """
    def __init__(self, identifier, name, value):
        self.identifier = identifier  # Deve ser uma instância de Identifier (value object)
        self.name = name
        self.value = value

    def __repr__(self):
        return f"<PipedriveEntity id={self.identifier} name={self.name} value={self.value}>"
