class KiteLoginError(Exception):
    """
    Base class for exceptions related to KiteLogin.
    """

    pass


class MissingSectionError(KiteLoginError):
    """
    Raised when a required section is missing from the ini file.
    """

    pass


class MissingKeyError(KiteLoginError):
    """
    Raised when a required key within a section is missing from the ini file.
    """

    pass
