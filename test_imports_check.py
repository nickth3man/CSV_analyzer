try:
    from src.backend import main
    print("Import src.backend.main success")
except ImportError as e:
    print(f"Import src.backend.main failed: {e}")

try:
    from src.backend.nodes import analysis
    print("Import src.backend.nodes.analysis success")
except ImportError as e:
    print(f"Import src.backend.nodes.analysis failed: {e}")
