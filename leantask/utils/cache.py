import pickle
from typing import Any

from ..context import GlobalContext
from .string import generate_uuid


def save_cache(obj: Any) -> str:
    cache_id = generate_uuid()
    cache_file_path = GlobalContext.cache_dir() / (cache_id + '.pkl')

    with open(cache_file_path, 'wb') as f:
        pickle.dump(obj, f)

    return cache_id


def load_cache(cache_id: str):
    cache_file_path = GlobalContext.cache_dir() / (cache_id + '.pkl')
    with open(cache_file_path, 'rb') as f:
        return pickle.load(f)


def clear_cache(cache_id: str):
    cache_file_path = GlobalContext.cache_dir() / (cache_id + '.pkl')
    cache_file_path.unlink()
