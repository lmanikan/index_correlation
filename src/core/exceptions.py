"""Custom exceptions for the ETL pipeline."""

class ETLException(Exception):
    """Base exception for all pipeline errors."""
pass

class ExtractionError(ETLException):
    """Error during data extraction phase."""
pass

class ValidationError(ETLException):
    """Error during data validation."""
pass

class TransformationError(ETLException):
    """Error during correlation calculation."""
pass

class LoadingError(ETLException):
    """Error during result loading/writing."""
pass

class ConfigurationError(ETLException):
    """Error in configuration or setup."""
pass
