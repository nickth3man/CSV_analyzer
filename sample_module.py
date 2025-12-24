"""Sample module to test the linting and type checking configuration."""


def greet(name: str, greeting: str | None = None) -> str:
    """Generate a greeting message.

    Args:
        name: The name of the person to greet.
        greeting: Custom greeting message. Defaults to "Hello".

    Returns:
        A formatted greeting string.
    """
    if greeting is None:
        greeting = "Hello"
    return f"{greeting}, {name}!"


def calculate_area(length: float, width: float) -> float:
    """Calculate the area of a rectangle.

    Args:
        length: The length of the rectangle.
        width: The width of the rectangle.

    Returns:
        The area of the rectangle.

    Raises:
        ValueError: If length or width is negative.
    """
    if length < 0 or width < 0:
        raise ValueError("Length and width must be non-negative")
    return length * width


class Calculator:
    """A simple calculator class for basic operations."""

    def __init__(self) -> None:
        """Initialize the calculator."""
        self.history: list[str] = []

    def add(self, a: float, b: float) -> float:
        """Add two numbers.

        Args:
            a: First number.
            b: Second number.

        Returns:
            Sum of a and b.
        """
        result = a + b
        self.history.append(f"Added {a} + {b} = {result}")
        return result

    def get_history(self) -> list[str]:
        """Get calculation history.

        Returns:
            List of calculation history entries.
        """
        return self.history.copy()
