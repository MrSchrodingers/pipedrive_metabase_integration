class Identifier:
    """
    Objeto de valor que encapsula um identificador único.
    """
    def __init__(self, value):
        if not value:
            raise ValueError("O valor do identificador não pode ser vazio.")
        self.value = value

    def __eq__(self, other):
        if isinstance(other, Identifier):
            return self.value == other.value
        return False

    def __str__(self):
        return str(self.value)
