class IngestionError(Exception):
    """Base exception for this library."""

    pass


class APIError(IngestionError):
    """Raised when the backend API returns an error status."""

    def __init__(self, message, status_code=None, details=None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


class FileConfigurationError(IngestionError):
    """Raised for issues with local files (not found, bad format)."""

    pass
